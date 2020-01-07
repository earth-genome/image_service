"""Routines to manipulate outlines of political geographic divisions.

External class: CountyBoundaries

This class is built for use in wtl_service to locate stories within given
counties or states, but the methods may be useful for one-off manipulation of
relevant geojsons. E.g. to write to file the boundaries of Yolo and Stanislaus 
counties: 

>>> from geobox import counties
>>> cb = counties.CountyBoundaries
>>> cb.write_to_geojson(outfile.geojson', 
                        cb.combine_counties(['Yolo', 'Stanislaus'], 'CA'))  

Counties for fifty U.S. states and DC are supported via the data in
us_county_geojson.csv, from source:

    https://community.periscopedata.com/t/80k8f8/us-county-geojson

When loaded into pandas the dataframe has columns:
Index(['county', 'state_code', 'json_object'], dtype='object')

'county' is a string, e.g. 'Alameda'.
'state_code' is a postal code, e.g. 'CA'.
'json_object' is a geojson 'geometry' as a dumped string in EPSG:4326. 

The class can be used on another dataset with comparable structure
by setting class attributes which hold names to reference the  
columns in the dataframe:

Political division -> stateidx 
Subdivision -> countyidx
Json Object -> jsonidx.  

Instead of a CSV file, a dataframe can be input directly via class 
instantiation.

"""
import json
import os

import pandas as pd
import shapely
import shapely.ops

class CountyBoundaries(object):
    """Class to manipulate boundaries of political geographic divisions.

    Attributes:
        df: A pandas dataframe listing json_objects of political subdivisions.
        stateidx: Column name for political division.
        countyidx: Column name for subdivisions.
        jsonidx: Column name for geojsons as dumped strings.

    External methods:
        get_statenames: Return a list of known state names.
        get_countynames: Return a dict of states and their county names.
        write_to_geojson: Write a shapely geom to outfile as a geojson.
        combine_states: Find a shapely object for the union of states' 
            boundaries.
        combine_counties: Find a shapely object for the union of counties' 
            boundaries.
        get_state: Get a shapely polygon for input state.
        get_county: Get a shapely polygon for input county in given state.

    """

    def __init__(self, stateidx='state_code', countyidx='county',
                 jsonidx='json_object', csv=None, dataframe=None):
        if dataframe:
            self.df = dataframe
        elif csv:
            self.df = pd.read_csv(csv)
        else:
            raise ValueError('A dataframe or csv file is required.')
        self.stateidx = stateidx
        self.countyidx = countyidx
        self.jsonidx = jsonidx

    def get_statenames(self):
        """Return a list of known state names."""
        states = set(self.df[self.stateidx].tolist())
        return sorted(list(states))

    def get_countynames(self):
        """Return a dict of states and their county names."""
        states = self.get_statenames()
        counties = {}
        for s in states:
            statedf = self._state_df(s)
            statedf = statedf.sort_values(self.countyidx)
            counties.update({s:statedf[self.countyidx].tolist()})
        return counties

    def write_to_geojson(self, outfile, geom):
        """Write a shapely geom to outfile as a geojson."""
        with open(outfile, 'w') as f:
            json.dump(shapely.geometry.mapping(geom), f)

    def combine_states(self, states):
        """Find a shapely object for the union of states' boundaries."""
        state_geoms = [self.get_state(s) for s in states]
        return self._merge(state_geoms)
        
    def combine_counties(self, counties, state):
        """Find a shapely object for the union of counties' boundaries."""
        county_geoms = [self.get_county(co, state) for co in counties]
        return self._merge(county_geoms)

    def get_state(self, state):
        """Get a shapely polygon for input state."""
        statedf = self._state_df(state)
        county_geojsons = [json.loads(g) for g
                               in statedf[self.jsonidx].tolist()]
        county_geoms = [shapely.geometry.asShape(g) for g in county_geojsons]
        return self._merge(county_geoms)
    
    def get_county(self, county, state):
        """Get a shapely polygon for input county in given state.""" 
        countydf = self._county_df(county, state)
        geojson = json.loads(next(iter(countydf[self.jsonidx].tolist())))
        return shapely.geometry.asShape(geojson)

    def _state_df(self, state):
        """Extract a single state's dataframe from self.df."""
        statedf = self.df[self.df[self.stateidx]==state]
        if statedf.empty:
            raise ValueError('No data found for state {}'.format(state))
        return statedf

    def _county_df(self, county, state):
        """Extract a single county's dataframe from self.df."""
        statedf = self._state_df(state)
        countydf = statedf[statedf[self.countyidx]==county]
        if countydf.empty:
            raise ValueError('No data found for county {} in state {}'.format(
                county, state))
        return countydf

    def _merge(self, geoms):
        return shapely.ops.unary_union(geoms)
