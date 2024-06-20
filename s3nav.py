from pathlib import Path, PurePath
from minio.deleteobjects import DeleteObject
from time import time
import click
from skin import _e, _i, _p
from pfs import PsuedoFileSystem, fmt_date, fmt_size, rm
from s3core import get_client, lswild


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





def do_rm(client, bucket, pattern):
    """ 
    Wild card removal of files in object store.
    Be careful
    """
    files = lswild(client, bucket, pattern, objects=True)
    if len(files) == 0:
        click.echo(_i(f'Nothing to delete matching [{pattern}] found in [{bucket}] in [{client.alias_name}]'))
        return
    rm(client, bucket, files)

def do_ls(client, bucket, pattern, columns=None, size=None, date=None):
    """
    Wild card listing of files in object store
    """
    files = lswild(client, bucket, pattern)
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
        objects = lswild(client, bucket, pattern, objects=True)
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
        files = lswild(client, bucket, pattern)

    if len(files):
        for file in files:
            print(file)
    else:
        print(f'No files found matching "{pattern}" in [{bucket}] at [{client.alias_name}]')
        

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

    
    pfs = PsuedoFileSystem(target)

if __name__ == "__main__":
   cli()
