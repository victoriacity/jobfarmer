import os, json, csv
import numpy as np
from workflow import Job

class ParameterArrayBuilder():
    def __init__(self, base_dir, path_seperator=''):
        self.param_list = []
        self.key_list = []
        self.base_dir = base_dir
        self.path_idx = ''
        self.path_sep = path_seperator
        self.repeat = 0
        self.instnames = []
        self.insts = []
        self.setter = None

    def set_repeat(self, repeat, newfolder=False):
        if repeat != int(repeat) or repeat <= 0:
            raise ValueError("Invalid repeats:", repeat)
        self.repeat = repeat
        self.path_idx = '%i'
        if newfolder:
            self.path_idx = os.path.join('_', self.path_idx)[1:]
        else:
            self.path_idx = self.path_sep + self.path_idx

    def check(self):
        if not self.param_list:
            raise Exception("Haven't added parameters yet; use self.add_parameter(*)")
        if not self.setter:
            raise Exception("Haven't added setter yet; use self.add_setter(*)")

    def _get(self):
        if len(self.insts) == 1 and self.instnames[0] == '':
            path_struc = '%s%s' + self.path_idx
        else:
            path_struc = '%s' + self.path_sep + '%s' + self.path_idx
        grid = ParameterArray(self.base_dir, self.param_list, 
                       self.key_list, path_struc, self.setter,
                       self.repeat)
        grid.set_instances(self.instnames, self.insts)
        return grid

    def get(self):
        raise NotImplementedError()
    
    def add_instance(self, name, job_inst):
        if name in self.instnames:
            raise ValueError('Instances should be unique!')
        self.instnames.append(name)
        self.insts.append(job_inst)
    
    def add_setter(self, setter):
        if self.setter:
            print("Overwriting setter")
        self.setter = setter

    def add_parameter(self, param, key):
        raise NotImplementedError()

class GridBuilder(ParameterArrayBuilder):
    def __init__(self, base_dir, path_seperator=''):
        super().__init__(base_dir, path_seperator)
        self.values_all = []
    
    def add_parameter(self, param, key, values):
        self.param_list.append(param)
        self.key_list.append(key)
        self.values_all.append(values)

    def get(self):
        self.check()
        self.param_list, self.key_list = mesh(self.param_list, self.key_list, 
                                                *self.values_all, sep=self.path_sep)
        return super()._get()

class RandomBuilder(ParameterArrayBuilder):
    def __init__(self, base_dir, path_seperator=''):
        self.vmin = []
        self.vmax = []
        self.modifier = None
        super().__init__(base_dir, path_seperator)
    
    def add_parameter(self, param, key, vmin, vmax):
        self.param_list.append(param)
        self.key_list.append(key)
        self.vmin.append(vmin)
        self.vmax.append(vmax)

    def set_modifier(self, modifier):
        self.modifier = modifier

    def get(self, npoints):
        self.check()
        self.param_list, self.key_list = random(self.param_list, self.key_list, 
                                                np.array(self.vmin), np.array(self.vmax),
                                                npoints)
        return super()._get()

def mesh(param_names, keys, *param_vals, sep=''):
    key_list = []
    param_list = []
    arr = np.array(np.meshgrid(*param_vals, indexing='ij')).reshape(len(param_vals), -1)
    idx = [list(range(len(param_vals[i]))) for i in range(len(param_vals))]
    idx = np.array(np.meshgrid(*idx, indexing='ij')).reshape(len(param_vals), -1)
    for i in range(idx.shape[1]):
        param_list.append({k: v for k, v in zip(param_names, arr[:, i])})
        key_list.append(sep.join(["%s%i" % (k, z) for k, z in zip(keys, idx[:, i])]))
    return param_list, key_list

def random(param_names, keys, vmin, vmax, npoints):
    mean, std = (vmin + vmax) / 2, vmax - vmin
    vals = np.random.rand(npoints, len(param_names))
    vals *= std.reshape(1, -1)
    vals += mean.reshape(1, -1)
    key_list = []
    param_list = []
    for i in range(vals.shape[0]):
        param_list.append({k: v for k, v in zip(param_names, vals[i, :])})
        key_list.append(''.join(keys) + str(i))
    return param_list, key_list


class ParameterArray:
    def __init__(self, base_dir, parameters, keys, path_struc, setter, repeat=0):
        self.base_dir = base_dir
        self.parameters = parameters
        self.path_struc = path_struc
        self.repeat = repeat
        self.instances = []
        self.inst_names = []
        self.jobs = []
        self.keys = keys
        self.setter = setter
        path_size = len(self.path_struc.split('%'))
        if path_size != 4 and repeat or path_size != 3 and not repeat:
            raise ValueError('invalid path structure %s' % self.path_struc)
    
    def get_path(self, instname, key, idx):
        if idx:
            subdir = self.path_struc % (instname, key, idx)
        else:
            subdir = self.path_struc % (instname, key)
        return os.path.join(self.base_dir, subdir)

    def set_instances(self, names, insts):
        self.instances = insts
        self.inst_names = names
    
    def make(self):
        for inst, name in zip(self.instances, self.inst_names):
            for key, param in zip(self.keys, self.parameters):
                for idx in range(self.repeat > 0, self.repeat + 1):
                    job_dir = self.get_path(name, key, idx)
                    newjob = inst.copy(job_dir)
                    self.setter.set(newjob, param)
                    self.jobs.append(newjob.__dict__)
        return JobArray(self.base_dir, self.jobs)

    def save_csv(self, file):
        n_params = len(self.parameters[0].keys())
        with open(file, 'w+') as f:
            writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['id', 'name'] + list(self.parameters[0].keys()) + ['path'])
            i = 0
            for name in self.inst_names:
                for key, param in zip(self.keys, self.parameters):
                    for idx in range(self.repeat > 0, self.repeat + 1):
                        job_dir = self.get_path(name, key, idx)
                        writer.writerow([i, name] + list(param.values()) + [job_dir])
                        i += 1


class JobArray:

    def __init__(self, root_dir, jobs_dict=None, job_save='job.json', array_save='jobarray.json'):
        if not jobs_dict:
            self.jobs_dict = []
        else:
            self.jobs_dict = jobs_dict
        self.root = root_dir
        self.subdirs = [x['dir'].strip(self.root) for x in self.jobs_dict]
        self.job_save = job_save
        self.array_save = array_save
        if job_save:
            for d in self.jobs_dict:
                d['save_file'] = job_save

    def __getitem__(self, key):
        job = Job()
        job.load_dict(self.jobs_dict[key])
        return job

    def __len__(self):
        return len(self.jobs_dict)

    def refresh(self):
        self.jobs_dict = [self[i].read_status().__dict__ for i in range(len(self))]
        return self
    
    def save_all(self):
        with open(os.path.join(self.root, self.array_save), 'w+') as f:
            for job in self.jobs_dict:
                f.write("%s\n" % job['dir']) 
        for i in range(len(self)):
            self[i].save(self[i].save_file)
        return self
    
    def load_all(self, loadfile=None):
        loadfile = loadfile or self.array_save
        saves = []
        with open(os.path.join(self.root, loadfile), 'r') as f:
            for line in f.readlines():
                saves.append(os.path.join(line.strip(), self.job_save))
        if self.jobs_dict:
            raise ValueError("Can load only from empty job list!")
        for s in saves:
            with open(s, 'r') as f:
                self.jobs_dict.append(json.load(f))
        return self

        
    def submit(self):
        self.save_all()
        for i in range(len(self)):
            self[i].submit()
    
    def report(self, verbose=0):
        self.refresh()
        status = {Job.PENDING:0, Job.RUNNING:0, Job.COMPLETED:0, Job.ERROR:0}
        for i in range(len(self)):
            status[self[i].report()] += 1
        if verbose:
            print("Total jobs: %d" % len(self))
            print("Pending: %d, Running: %d, Completed: %d, Error: %d" \
                % (status[Job.PENDING], status[Job.RUNNING], status[Job.COMPLETED], status[Job.ERROR]))
        if verbose > 1:
            for i in range(len(self)):
                j = self[i]
                if j.report() == Job.ERROR:
                    print("Job %s error at state %s" % (j.dir, j.states[j.cur_state]))
        return status

    def logerror(self, file):
        lines = [self[i].dir + "\n" for i in range(len(self)) if self[i].report() == Job.ERROR]
        if lines:
            with open(os.path.join(self.root, file), 'w+') as f: 
                f.writelines(lines)
                    

if __name__ == "__main__":
    pass
