from pathlib import Path, PurePath
from upload import get_user_config
from minio import Minio
from minio.deleteobjects import DeleteObject
from time import time
import click
import argparse, shlex

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

def _handle_argument(target):
    """
    Extract the interesting stuff from the simple command target argument
    """
    p = PurePath(target)
    parts = p.parts
    alias = parts[0]
    bucket = parts[1]
    therest = '/'.join(list(parts[2:])) 
    client = get_client(alias)
    return client, bucket, therest

def _lswild(client, bucket, pattern='*', objects=False):
    """ 
    Do an ls on a bucket visible on the minio client which matches pattern
    This isn't quite a perfect glob! So be careful. Also, we're being cunning
    in trying to get the server to try and do some of the matching, at least
    for simple cases.
    If objects is False, retunr just names, oterwise return the objects
    for later processing
    """
    
    asterix = pattern.find('*')
    
    if asterix > 0:
        prefix = pattern[0:asterix]
        pattern = pattern[asterix:]
    else:
        prefix = None
    
    objects = client.list_objects(bucket, prefix=prefix)
    
    if objects:
        olist = [o for o in objects if Path(o.object_name).match(pattern)]
        return olist
    else:
        object_names = [o.object_name for o in objects]
        flist = [p for p in object_names if Path(p).match(pattern)]
        return flist

def get_client(alias):
    """
    Get Minio client from the configuration alias, and patch the 
    client with that alias name
    """
    credentials = get_user_config(alias)
    secure = False
    if credentials['url'].startswith('https'):
        secure = True
    try:
        api = {'endpoint':'url','access_key':'accessKey','secret_key':'secretKey'}
        try:
            kw = {k:credentials[v] for k,v in api.items()}
        except KeyError:
            raise KeyError(f"Cannot find {v} in credentials supplied")
        kw['secure'] = secure
        endpoint = kw['endpoint']
        slashes = endpoint.find('//')
        if slashes > -1:
            kw['endpoint'] = endpoint[slashes+2:]
        client = Minio(**kw) 
    except:
        raise 
    # nasty monkey patch, but I want to carry this around
    client.alias_name = alias
    return client

def do_rm(client, bucket, pattern):
    """ 
    Wild card removal of files in object store.
    Be careful
    """
    files = _lswild(client, bucket, pattern)
    if len(files) == 0:
        print(f'No files to delete matching [{pattern}] found in [{bucket}] in [{client.alias_name}]')
        return
    print(_p('**\nManifest of files to delete\n**'))
    for file in files:
        print(file.object_name)
    if click.confirm(_p('Delete these files from {bucket}?'), default=False):
        delete_list = [DeleteObject(file.object_name) for file in files]
        errors = list(client.remove_objects( bucket, delete_list))
        if errors != []:
            for error in errors:
                print("error occurred when deleting object", error)
            lf = len(files)
            le = len(errors)
            print(f"{lf-le}/{lf} files deleted from {bucket} in {client.alias_name}")   
            print('You will need to check which files were actually deleted')
        else:
            print(f"{len(files)} files deleted from {bucket} in {client.alias_name}")    

def do_ls(client, bucket, pattern, columns=None, size=None, date=None):
    """
    Wild card listing of files in object store
    """
    files = _lswild(client, bucket, pattern)
    if size is not None or date is not None:
        columns = 1
        d = ['Object Name']
        if size:
            d.append('Size')
        if date:
            d.append('Last Modified')
        data = [d]
        m = [len(x) for x in d]
        fmts = ['>' for x in d]
        fmts[0]='<'
        objects = _lswild(client, bucket, pattern, objects=True)
        for o in objects:
            d = [o.object_name]
            if size:
                d.append(fmt_size(o.size))
            if date:
                d.append(fmt_date(o.last_modified))
            data.append(d)
            m = [max(i,len(j)) for i,j in zip(m,d)]
        for i in range(1,len(m)):
            m[i]+=5
        files = [] 
        for row in data:
            files.append(' '.join(f"{item:{fmt}{max_len}}" for item, fmt, max_len in zip(row, fmts, m)))
    else:
        files = _lswild(client, bucket, pattern)

    if len(files):
        for file in files:
            print(file)
    else:
        print(f'No files found matching "{pattern}" in [{bucket}] at [{client.alias_name}]')




def _i(string):

        return click.style(string, fg='green')

def _e(string):
        return click.style(string, fg='blue')

def _p(string):
        return click.style(string, fg='magenta')



class PsuedoFileSystem:

    
    def __init__(self, alias, bucket, cwd):
        click.echo(_i('You have entered a lightweight management tool for organising "files" inside an S3 object store'))
        self.client = get_client(alias)
        self.alias = alias
        self.buckets = [b.name for b in self.client.list_buckets()]
        if bucket is None or cwd is None:
            self.cb(bucket)
        else:
            
            if bucket not in self.buckets:
                raise NotADirectoryError(f'No bucket {bucket} - start with valid bucket name or none')
            self.bucket = bucket
            self.cd(cwd)
        self.path = ''

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


    def next(self, commands):
        click.echo(_i('Available commands: ') +_e(' '.join(commands)))
        command = click.prompt(_p('Enter'))
        bits = command.split(' ')
        if len(bits) == 1:
            bits.append(None)
        if bits[0] not in commands:
            click.echo(_i('Pleae enter one of the available commands: ')+ _p(" ".join(commands)))
            self.next(commands)
        if bits[0] == 'exit':
            exit()
        match bits[0]:
            case 'cb': 
                self.cb(bits[1])
            case 'ls':
                self.ls(bits[1])
            case 'cd':
                self.cd(bits[1])
            case 'rm':
                self.rm(bits[1:])
        click.echo(_i('Command Not Understood'))
        self.next(commands)
    def cb(self, bucket):
        if bucket is None:
            click.echo(_p('Location') + f': {self.alias}')
            click.echo(_p('Available Buckets') + f': {" ".join(self.buckets)}')
            commands = ['cb','exit']
            self.next(commands)
        if bucket not in self.buckets:
            click.echo(_p(f'Bucket [{bucket}] does not exist'))
        else:
            self.bucket = bucket
            volume, nfiles, ndirs, mydirs, myfiles = self._recurse('')
            click.echo(_i('Bucket: ') + bucket + _i(' contains ')+ fmt_size(volume) + _i(' in ') + str(nfiles) + _i(' files/objects.'))
        commands = ['cb','cd','exit']
        self.next(commands)

    def cd(self,path):
        if path is None:
            path = ''
        if path == '..':
            if self.path != '':
                bits = self.path.split('/')
                del bits[-2:]
                path = '/'.join(bits)
        if path != '' and not path.endswith('/'):
            path+='/'
        
        self.path = path
        volume, nfiles, ndirs, mydirs, myfiles = self._recurse(path)
        if path == '':
            path = '/'
        click.echo(_i('Location: ') + path + _i(' contains ')+ fmt_size(volume) + _i(' in ') + str(nfiles) + _i(' files/objects.'))
        click.echo(_i('This directory contains ')+ str(len(myfiles)) + _i(' files and ') + str(len(mydirs)) + _i(' directories.'))
        if len(mydirs) > 0:
            click.echo(_i('Sub-directories are : ')+_e(' '.join([f'{d[0]}({d[1]})' for d in mydirs])))
        commands = ['ls','cd','cb','exit']
        self.next(commands)

    def ls(self, extras):

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
        mlen+=1
        for f in myfiles:
            click.echo(f'{_pstrip(f[0]):<{mlen}} '+_e(f'{f[1]:>10}') +f'   {f[2]}')   
        if len(mydirs) > 0:  
            click.echo(_i('Sub-directories are : ')+_e(' '.join([f'{d[0]}({d[1]})' for d in mydirs])))   
        commands = ['ls','cd','cb','exit','rm']
        self.next(commands)
    
    def rm(self, extras, collect = False):

        path = self.path + ''.join(extras)
        do_rm(self.client, self.bucket, path)
        commands = ['ls','cd','cb','exit','rm']
        self.next(commands)
        

@click.group()
def cli():
    pass

@cli.command()
@click.argument('action')
def rm(action):
    client, bucket, therest = _handle_argument(action)
    do_rm(client, bucket, therest)

@cli.command()
@click.argument('action')
def ls(action):
    client, bucket, therest = _handle_argument(action)
    do_ls(client, bucket, therest)

@cli.command()
@click.argument('target')
def manage(target):
    """ This provides a command line pseudo shell method for managing files """
   
    bits = target.split('/')
    if len(bits) > 2:
        cwd = "/".join(bits[2:])
        bucket = bits[1]
        alias = bits[0]
    elif len(bits) == 2: 
        cwd = ""
        bucket = bits[1]
        alias = bits[0]
    else:
        bucket = None
        cwd = ""
        alias = target
    
    pfs = PsuedoFileSystem(alias, bucket, cwd)

if __name__ == "__main__":
   cli()
