import click

# half an idea to support skinning the prompts in the future

def __style(string, col):
    """ Colour a string with a particular style """
    return click.style(string,fg=col)

def _i(string, col='green'):
    """ Info string """
    return __style(string,col)
    
def _e(string, col='blue'):
    """ Entity string """
    return __style(string,col)

def _p(string, col='magenta'):
    """ Prompt String """
    return __style(string,col)
    