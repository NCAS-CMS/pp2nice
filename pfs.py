import click
from skin import _i, _e, _p
from s3core import get_client, lswild
from minio.deleteobjects import DeleteObject
from pathlib import Path
from minio.commonconfig import CopySource

def fmt_size(num, suffix="B"):
    """ Take the sizes and humanize them """
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def fmt_date(adate):
    """ Take the reported date and humanize it"""
    return adate.strftime('%Y-%m-%d %H:%M:%S %Z')

def rm(client, bucket, objects):
     """ 
     Remove a list of objects 
     """
     click.echo(_i('\nList of objects for deletion:'))
     for o in objects:
         click.echo(_e(o.object_name))
     if click.confirm(_p('Delete these files from {bucket}?'), default=False):
        delete_list = [DeleteObject(o.object_name) for o in objects]
        # this would be lazy if I didn't force it with the list and error parsing
        errors = list(client.remove_objects( bucket, delete_list))
        if errors != []:
            for error in errors:
                print("error occurred when deleting object", error)
            lf = len(objects)
            le = len(errors)
            click.echo(_p(f"{lf-le}/{lf} files deleted from {bucket} in {client.alias_name}"))
            click.echo(_p('You will need to check which files were actually deleted'))
        else:
            click.echo(_i(f"{len(objects)} objects deleted from {bucket} in {client.alias_name}"))


        


class PsuedoFileSystem:
    """
    Provides a limited shell for working with S3 buckets as if they were a posix like file system
    """
    def __init__(self, target):
        """
        Can be instantiated with just a minio alias, or a specific bucket or bucket/path.
        """
        click.echo(_i('You have entered a lightweight management tool for organising "files" inside an S3 object store'))
        bits = target.split('/')

        self.alias = bits[0]
        print(self.alias)
        self.client = get_client(self.alias)

        self.buckets = [b.name for b in self.client.list_buckets()]

        match len(bits):

            case 1: 
                self.bucket = None
                self.path = None
                commands = ['cb','mb','exit']
            case 2:
                self.bucket = bits[1]
                self.path = None
                commands = ['cb','mb','exit']
            case 3:
                self.bucket = bits[1]
                self.path = bits[2]
                commands = self._cd_lander(self.path)
            case _:
                click.echo(_p('Initialisation not understood'))
                exit()
            
        self._next(commands)
        exit()



    def _recurse(self, path, match=None):
        """ 
        From a given path, head down the tree and do some summing
        """
        if path == "":
            prefix = None
        else:
            prefix = path
        objects = self.client.list_objects(self.bucket,prefix=prefix)
        if match is not None:
            objects = [o for o in objects if Path(o.object_name).match(match)]

        sum  = 0
        files = 0
        dirs = 1
        mydirs = []
        myfiles = []
        for o in objects:
            if o.is_dir:
                path = f'{path}/{o.object_name}'
                dsum, dfiles, ddirs, md, mf = self._recurse(path)
                sum += dsum
                files += dfiles
                dirs += 1
                mydirs.append([o.object_name, fmt_size(dsum)])
            else:
                sum += o.size
                files +=1
                myfiles.append([o.object_name, fmt_size(o.size), fmt_date(o.last_modified)])
        return sum, files, dirs, mydirs, myfiles

    def _next(self, commands):
        """ 
        Internal command to prompt for the next action after doing an action
        """
        cmd = None
        while cmd != 'exit':
            click.echo(_i('Available commands: ') +_e(' '.join(commands)))
            command = click.prompt(_p('Enter'))
            bits = command.split(' ')
            cmd = bits[0]
            if len(bits) == 1:
                bits.append(None)
            if cmd not in commands:
                click.echo(_i('Please enter one of the available commands: ')+ _p(" ".join(commands)))
            else:
                match cmd:
                    case 'exit':
                        exit()
                    case 'cb': 
                        commands = self.cb(bits[1])
                    case 'ls':
                        commands = self.ls(bits[1])
                    case 'cd':
                        commands = self.cd(bits[1])
                    case 'rm':
                        commands = self.rm(bits[1:])
                    case 'mv':
                        commands = self.mv(bits[1:])
                    case 'mb':
                        commands= self.mb(bits[1])
                    case _:
                        click.echo(_i('Command Not Understood'))
            
    def cb(self, bucket):
        """
        Change to a (new) bucket
        """
        if bucket is None:
            click.echo(_p('Location') + f': {self.alias}')
            click.echo(_p('Available Buckets') + f': {" ".join(self.buckets)}')
            commands = ['cb','mb','exit']
            return commands
        if bucket not in self.buckets:
            click.echo(_p(f'Bucket [{bucket}] does not exist'))
            return 
        else:
            self.bucket = bucket
            volume, nfiles, ndirs, mydirs, myfiles = self._recurse('')
            click.echo(_i('Bucket: ') + bucket + _i(' contains ')+ fmt_size(volume) + _i(' in ') + str(nfiles) + _i(' files/objects.'))
        commands = ['cb','cd','exit']
        return commands
        

    def cd(self,path):
        """
        Change position in bucket to expose contents as a directory
        """
        if path is None:
            path = ''
        if path == '..':
            if self.path != '':
                bits = self.path.split('/')
                del bits[-2:]
                path = '/'.join(bits)
        if path != '' and not path.endswith('/'):
            path+='/'
        return self._cd_lander(path)
        
    def _cd_lander(self, path):
        """
        This internal routine reports information about a particular path
        """
        self.path = path
        volume, nfiles, ndirs, mydirs, myfiles = self._recurse(path)
        if path == '':
            path = '/'
        click.echo(_i('Location: ') + path + _i(' contains ')+ fmt_size(volume) + _i(' in ') + str(nfiles) + _i(' files/objects.'))
        click.echo(_i('This directory contains ')+ str(len(myfiles)) + _i(' files and ') + str(len(mydirs)) + _i(' directories.'))
        if len(mydirs) > 0:
            click.echo(_i('Sub-directories are : ')+_e(' '.join([f'{d[0]}({d[1]})' for d in mydirs])))
        commands = ['ls','cd','cb','exit']
        return commands

    def ls(self, extras):
        """ 
        List the files and directories in a bucket, potentially with a wild card
        """
        def _pstrip(x):
            bits = x.split('/')
            return bits[-1]
        volume, nfiles, ndirs, mydirs, myfiles = self._recurse(self.path, extras)
        click.echo(_i('Location: ') + self.path + _i(' contains ')+ fmt_size(volume) + _i(' in ') + str(nfiles) + _i(' files/objects.'))
        directory = 'directory' 
        if extras: directory = "match"
        click.echo(_i(f'This {directory} contains ')+ str(len(myfiles)) + _i(' files and ') + str(len(mydirs)) + _i(' directories.'))
      
        mlen = 0
        for f in myfiles:
            lf = len(f[0])
            if lf > mlen:
                mlen = lf
        for f in myfiles:
            click.echo(f'{_pstrip(f[0]):<{mlen}}  '+_e(f'{f[1]:>10}') +f'   {f[2]}')   

        if len(mydirs) > 0:  
            click.echo(_i('Sub-directories are : ')+_e(' '.join([f'{d[0]}({d[1]})' for d in mydirs])))   
        commands = ['ls','cd','cb','exit','rm','mv']
        return commands
    
    def rm(self, extras):
        """ 
        Remove files according to a particular match, for a particular path
        """

        path = self.path + ''.join(extras)
        objects = lswild(self.client, self.bucket, path)
        rm(self.client, self.bucket, objects)
        return self.cd(path)

    def mb(self, bucket_name):
        """ 
        Make bucket, return error if existing
        """
        # update the list
        self.buckets = [b.name for b in self.client.list_buckets()]
        
        if bucket_name in self.buckets:
            click.echo(_p(f'Bucket {bucket_name} already exits'))
            
            self.cb(None)
        r = self.client.make_bucket(bucket_name)
        self.buckets.append(bucket_name)
        return self.cb(bucket_name)


    def mv(self, command):
        """
        Move files from one location to another (server side)
        This is an expensive operation!
        """
        try:
            source, target = tuple(command)
        except:
            click.echo(_p('Invalid mv command'))
            return self.cd(self.path)

        if target.startswith('/'):
            target_bucket = self.bucket
        else:
            bits = target.split('/')
            if bits[0] not in self.buckets:
                click.echo(_p('Invalid mv command: target must start with a bucket name or /'))
                return self.cd(self.path)
            target_bucket = bits[0]

        if target.endswith('/'):
            singleton = False
        else:
            if source.find('*') > -1 or source.endswith('/'):
                click.echo(_p('Cannot move multiple files to a target that is not a directory'))
                return self.cd(self.path)
                singleton= True


        path = self.path + ''.join(source)
        objects = lswild(self.client, self.bucket, path, objects=True)
        if singleton:
            if len(objects) != 1:
                click.echo(_p('Unexpected error cannot mv multiple files to one file'))
                return self.cd(self.path)
            targets = [target]
        else:
            targets = [f'{target}/{o.object_name}' for o in objects]
        click.echo(_i('\nList of movements:'))
        for o,t in zip(objects,targets):
            click.echo(_e(f'mv {o.object_name} to {t}'))
        volume = fmt_size(sum([o.size for o in objects]))
        print(_p('This move is done as a server side copy - it is not "just" a rename!'))
        if click.confirm(_p(f'Move these files ({volume}) ?'), default=False):
            for o,t in zip(objects, targets):
                src = CopySource(self.bucket, o.object_name)
                result = self.client.copy_object(self.bucket, t, src)
                if singleton:
                    if o.size != result.size:
                        click.echo(_p(f'Failed copy of {o.object_name} - mv operation terminated'))
                        self.cd(self.path)
                if not singleton and result.object_name != o.object_name:
                    click.echo(_p(f'Failed copy of {o.object_name} - mv operation terminated'))
                    self.cd(self.path)
                self.client.remove_object(self.bucket,o.object_name)
                click.echo(f'Created {_e(result.object_name)}')
            
        return self.cd(self.path)