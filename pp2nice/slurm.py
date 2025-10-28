class SlurmLogger:
    """ 
    Replaces python logging module which I simply cannot get to work on Slurm
    in such a way that we get output as the job runs. I tried FileHandlers
    with flush, and StreamHandlers with flush, and nada ... this works.
    """
    def __init__(self,format=f'%(asctime)s:  %(message)s [[%(funcname)s]]',datefmt='%y-%b-%d %H:%M:%S'):
        self.format = format
        self.datefmt = datefmt

    def info(self,message):
        asctime = datetime.datetime.now().strftime(self.datefmt)
        funcname = inspect.stack()[1].function
        values = {'asctime':asctime, 'funcname':funcname, 'message': message}
        if message[-1:]=='\n':
            # the message wanted a blank line, we should honour that
            values['message']=message[0:-1]
            output = self.format % values + '\n'
        else:
            output = self.format % values
        print(output)