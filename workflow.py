import os, shutil, subprocess, json
from copy import deepcopy

def load_job(file):
    job = Job()
    with open(file, 'r') as f:
        job.load_dict(json.load(f))
        job.get_status()
        return job

class Job:

    PENDING = 0
    RUNNING = 1
    COMPLETED = 2
    ERROR = 3

    BACKUP = 'python /home/andrewsun/scripts/jobfarmer/backup.py'
    LOG = 'echo %i: %s >> %s\n'
    submit_cmd = 'qsub'

    def __init__(self, workdir='.', script='.job.sh', log='.log'):
        '''
        Initializes the job
        '''
        self.dir = os.path.abspath(workdir)
        self.input = []
        self.output = []
        self.checkpoint = []
        self.states = []
        self.cur_state = 0
        self.running = False
        self.error_exit = False
        self.cmd = []
        self.script = os.path.join(self.dir, script)
        self.submit_template = None
        self.submit_cmd = None
        self.log = os.path.join(self.dir, log)
        self.save_file = None
        
    def chdir(self, dir):
        olddir = self.dir
        self.dir = os.path.abspath(dir)
        self.script = os.path.join(self.dir, os.path.basename(self.script))
        self.log = os.path.join(self.dir, os.path.basename(self.log))
        if self.save_file:
            self.save_file = os.path.join(self.dir, os.path.basename(self.save_file))
        for i in range(len(self.cmd)):
            self.cmd[i] = self.cmd[i].replace(olddir, self.dir)


    def add_input(self, input_file):
        self.input.append(input_file)
        return self

    def add_output(self, output_file):
        self.output.append(output_file)
        return self

    def add_checkpoint(self, checkpoint_file):
        self.checkpoint.append(checkpoint_file)
        return self

    def missing_input(self):
        for f in self.input + self.checkpoint:
            if not os.path.exists(os.path.join(self.dir, f)):
                return os.path.join(self.dir, f)
        return None
    
    def get_file_list(self):
        return self.input + self.output + self.checkpoint

    def get_files(self):
        return self.input, self.output, self.checkpoint
    
    def get_dir(self):
        return self.dir

    def add_program(self, name, cmd, backup=True):
        if name in self.states:
            raise ValueError("program alias already exists!")
        self.states.append(name)
        self.cmd.append(cmd)
        if backup:
            self.states.append('bak_' + name)
            self.cmd.append('%s %s %s ' % (self.BACKUP, self.dir, name) + ' '.join(self.get_file_list()))

    def change_program(self, name, cmd, backup=True):
        idx = self.states.index(name)
        self.cmd[idx]=(cmd)
        if backup and (idx == len(self.states) - 1 or self.states[idx + 1] != 'bak_' + name):
            self.states.insert(idx + 1, 'bak_' + name)
            self.cmd.insert(idx + 1, '%s %s %s ' % (self.BACKUP, self.dir, name) + ' '.join(self.get_file_list()))
        if not backup and idx < len(self.states) - 1 and self.states[idx + 1] == 'bak_' + name:
            self.states.pop(idx + 1)
            self.cmd.pop(idx + 1)

    def set_submit_template(self, file):
        self.submit_template = os.path.abspath(file)

    def set_submit_command(self, cmd):
        self.submit_cmd = cmd

    def compose(self):
        if not self.save_file:
            raise Exception("Job should be saved before writing submission script")
        else:
            self.save()
        if not self.submit_template:
            code = 'w+'
        else:
            code = 'a+'
            shutil.copy(self.submit_template, self.script)
        with open(self.script, code) as f:
            # always enter the job directory
            f.write('cd %s \n' % self.dir)
            f.write('echo Starting job %s > %s\n' % (str(self), self.log))
            for i in range(self.cur_state, len(self.states)):
                c = self.cmd[i]
                f.write("%s\n" % c)
                f.write("if [ $? != \"0\" ]; then echo ERROR on %s >> %s; exit $?; fi\n" % (c, self.log))
                f.write(Job.LOG % (i, c, self.log))
                
    def clone_files(self, dest):
        os.makedirs(dest, exist_ok=True)
        for fr in self.get_file_list():
            if os.path.exists(os.path.join(self.dir, fr)):
                shutil.copy(os.path.join(self.dir, fr), os.path.join(dest, fr),
                            follow_symlinks=False)

    def copy(self, dest, copyfiles=True):
        newjob = Job()
        newjob.load_dict(deepcopy(self.__dict__))
        newjob.chdir(dest)
        if copyfiles:
            self.clone_files(dest)
        return newjob

    def submit(self):
        self.compose()
        os.chdir(self.dir)
        subprocess.run([self.submit_cmd, self.script])

    def save(self, file='job.json'):
        if file:
            self.save_file = os.path.join(self.dir, file)
        elif not self.save_file:
            raise ValueError("Save file should be specified")
        with open(self.save_file, 'w+') as f:
            json.dump(self.__dict__, f)

    def load_dict(self, d):
        for key in d:
            setattr(self, key, d[key])
    
    def read_status(self):
        self.error_exit = False
        try:
            with open(self.log, 'r') as f:
                lastline = None
                self.running = True
                for line in f:
                    s = line.strip().split(':')
                    if len(s) == 2:
                        self.cur_state = int(s[0]) + 1
                    lastline = line
                if self.cur_state == len(self.states):
                    self.running = False
                if lastline and lastline.split()[0] == 'ERROR':
                    self.error_exit = True
                    self.running = False
        except FileNotFoundError:
            self.cur_state = 0
            self.running = False
        return self

    def get_status(self):
        return self.read_status().cur_state

    def is_done(self):
        return self.get_status() == len(self.states)

    def report(self, verbose=0):
        self.get_status()
        if self.error_exit:
           return Job.ERROR
        if self.cur_state == len(self.states):
            report_code = Job.COMPLETED
        elif self.cur_state == 0 and not self.running:
            report_code = Job.PENDING
        else:
            report_code = Job.RUNNING
        if verbose:
            if report_code == Job.PENDING:
                print("Job has not started")
            elif report_code == Job.RUNNING:
                print("Job is running at state %s" % self.states[self.cur_state])
            elif report_code == Job.COMPLETED:
                print("Job completed successfully")
            elif report_code == Job.ERROR:
                print("Job error at state %s" % self.states[self.cur_state])
        return report_code

    def clean(self):
        if os.path.exists(self.log):
            os.remove(self.log)
        for name in self.states:
            if os.path.exists(os.path.join(self.dir, name)):
                shutil.rmtree(os.path.join(self.dir, name))


    def __str__(self):
        return '[%s]' % super().__str__()[1:-1]

class Setter:
    def set(self, job, parameters):
        raise NotImplementedError()