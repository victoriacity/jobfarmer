import sys
from jobarray import JobArray

if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
    else:
        arg = '.'
    jobs = JobArray(arg)
    jobs.load_all()
    jobs.report(verbose=2)
    jobs.logerror('error.txt')
    jobs.save_all()
