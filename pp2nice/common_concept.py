import json
from pathlib import Path
from pp2nice.load_asset import load_vocab
import cf


class CommonConcepts:
    def __init__(self):
        self.db = load_vocab('cmip6_common_concept.json')
        self.ncasdb = load_vocab('ncas_common_concept.yaml')
        cf.load_stash2standard_name() 
        self.stash = cf.stash2standard_name() 

    def _findnn(self, name):
        """ Look for a name in the NCAS common concept table"""
        try:
            return self.ncasdb['index'][name]
        except KeyError:
            raise NotImplementedError(f'No short name found for [[{name}]]')

    def _findsn(self, standard_name):
        """ 
        Find a standard name in the database
        """
        try:
            return self._dimension_collapse_options(self.db['index'][standard_name])
        except KeyError:
            return self._findnn(standard_name)


    def _dimension_collapse_options(self, options):
        """ 
        For a given standard name, there will be multiple possible
        common concepts. We can leave the issue of table and 
        cell methods to other bits of code, but here we collapse down 
        to options with the same dimensionality
        """
        dimension_view = {}
        for table,values in options.items():
            for option, value in values.items():
                if value['dimensions'] not in dimension_view:
                    dimension_view[value['dimensions']] = option
        return dimension_view

    def identify(self, field):
        """
        Entry point to find common concept name for a field
        """
        try:
            sn = field.standard_name
        except:
            sn = field.long_name
            print('Using long name option for', sn)

        try:
            dboutput = self._findsn(sn)
            if isinstance(dboutput,str):
                return dboutput
            # easy option
            options = list(set([v for k,v in dboutput.items()]))
            if len(options)  == 1:
                return options[0]
            # options with multiple possibilities
            coordinate_types = [c.ctype for c in field.coordinates().values()]
            if 'Z' in coordinate_types:
                zdata = field.coordinate('Z').data
                if len(zdata) > 1:
                    for k,v in dboutput.items():
                        if 'alevel' in k:
                            return v
                else:
                    # which level is it?
                    level_options = {
                        '[10.0] m':'height10m',
                        '[500.0] hPa':'p500',
                        '[1.5] m': 'height2m'
                    }
                    try:
                        option = level_options[str(zdata)]
                    except KeyError:
                        raise NotImplementedError(f"Can't find {str(zdata)} in {level_options} for {sn}")
                    for k,v in dboutput.items():
                        if option in k:
                            return v
            else:
                surface_and_time = 'longitude latitude time'
                if set(coordinate_types) == {'X','Y','T'} and surface_and_time in dboutput:
                    return dboutput[surface_and_time]  
            print(dboutput)
            return NotImplementedError
        except NotImplementedError as error:
            if 'um_stash_source' in field.properties():
                print('**Trapped**:',error)
                return f"UM_{field.get_property('um_stash_source')}"
            print('**\n**\n**\nThe following error was encountered trying to identify:')
            print(field)
            print('**\n**\n**')
            raise 




