import json
from pathlib import Path
import cf

COMMON_CONCEPT_JSONDIR = '/home/users/lawrence/hiresgw/hrcm'

ncas_common_concept = {
    # This may not be the way to do this.
    #
    'index':{
        'ertel_potential_vorticity':'epv',
        'air_pressure_at_sea_level':'psl', # psl is an alias issue, so this duplicate is ok
        'HEAVYSIDE FN ON P LEV/UV GRID':'hfnpuv',
        'TOTAL MOISTURE FLUX U  RHO GRID':'tmfurg',
        'TOTAL MOISTURE FLUX V  RHO GRID':'tmfvrg',
        'air_potential_temperature':'apt',
        'snowfall_amount':'snamt', # consider converting to flux to be CMIP consistent?
        'surface_eastward_sea_water_velocity':'swevs',
        'surface_northward_sea_water_velocity':'swnvs',
        'land_binary_mask': 'lbmask',  # this is turning up in annual means, shouldn't it be fx?
        'CCA WITH ANVIL AFTER TIMESTEP':'ccaanv',
        'BULK CLOUD FRACTION IN EACH LAYER':'laycldfblk',
        'sea_ice_albedo':'sia',
        'LAYER CLD LIQ RE * LAYER CLD WEIGHT':'cldlayliq',
        'LAYER CLOUD WEIGHT FOR MICROPHYSICS':'cldlaypxwgt',
        'CONV CLOUD LIQ RE * CONV CLD WEIGHT':'cldconwgt',
        'CONV CLOUD WEIGHT FOR MICROPHYSICS':'cldconpxwgt',
        'DROPLET NUMBER CONC * LYR CLOUD WGT':'clddnumwgt',
        'LAYER CLOUD LWC * LAYER CLOUD WEIGHT':'cldlaylwcwgt',
        '2-D RE DISTRIBUTION * 2-D RE WEIGHT':'cld2ddxw',
        'WEIGHT FOR 2-D RE DISTRIBUTION':'cld2dwgt',
        '2-D RE * WEIGHT - WARM CLOUDS ONLY':'cld2dwgtwc',
        'WEIGHT FOR WARM CLOUD 2-D RE':'cldwgt2dwc',
        'NET DN SW O SEA FLX BLW 690NM:SEA MN':'radnetseaflx1',
        'COLUMN-INTEGRATED Nd * SAMP. WEIGHT':'colndxwgt',
        'SAMP. WEIGHT FOR COL. INT. Nd':'colndwgt',
        'FOSSIL FUEL ORG C OPTIC DEPTH IN RAD':'radorgcod',
        'DUST OPTICAL DEPTH FROM PROGNOSTIC':'raddustod',
        'downward_heat_flux_in_sea_ice':'sihf',
        'SURFACE SENSIBLE  HEAT FLUX     W/M2':'hfss', # check this isn't surface_upward_sensible_heat_flux
        'surface_upward_water_flux':'wfs',
        'wind_mixing_energy_flux_into_sea_water':'wmefsw',
        'SUBLIM. FROM SURFACE (GBM)  KG/M2/TS':'slms',
        'EVAP FROM OPEN SEA: SEA MEAN KG/M2/S':'evoss',
        'SEAICE TOP MELT LH FLX:SEA MEAN W/M2':'sitmhf',
        'SPECIFIC HUMIDITY  AT 1.5M':'huss', # have assumed this is the CMIP6 quantity, which should have name "specific humidity"
        'surface_snow_melt_heat_flux':'snmhfs',
        'gross_primary_productivity_of_carbon':'cgpp',
        'CANOPY EVAPORATION ON TILES':'canevt',
        'soil_respiration_carbon_flux':'cflxsr',
        'water_sublimation_flux':'slwflx',
        'TURBULENT MIXING HT AFTER B.LAYER m':'tmxhgt', 
        'STABLE BL INDICATOR':"sbli",
        'STRATOCUM. OVER STABLE BL INDICATOR':'scsbli',
        'WELL_MIXED BL INDICATOR':'wmbli',
        'DECOUPLED SC. NOT OVER CU. INDICATOR':'scdncui',
        'DECOUPLED SC. OVER CU. INDICATOR':'scdcui',
        'CUMULUS-CAPPED BL INDICATOR':'ccbli',
        'SURFACE TILE FRACTIONS':'stf',
        'CANOPY WATER ON TILES          KG/M2':'cwot',
        'SUBLIMATION MOISTURE FLUX ON TILES':'smfot',
        'SHEAR-DRIVEN B.LAYER INDICATOR':'sdbli',
        'SUBLIM. SEAICE:SEA MEAN RATE KG/M2/S':'slsi',
    }
}

def verify_consistency():
    """ 
    Verify that the ncas common concept database does not overlap
    with the CMIP one.
    """
    c = CommonConcepts()
    names=[]
    for k,v in c.db['index'].items():
        for kk,vv in v.items():
            for n in vv:
                names.append(n)
    for n,v in ncas_common_concept['index'].items():
        if v in names:
            print('Duplicate for ',n,v)
        
class CommonConcepts:
    def __init__(self):
        jfile = Path(COMMON_CONCEPT_JSONDIR)/'cmip6_common_concept.json'
        with open(jfile,'r') as jdata:
            self.db = json.load(jdata)
        self.ncasdb = ncas_common_concept
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




