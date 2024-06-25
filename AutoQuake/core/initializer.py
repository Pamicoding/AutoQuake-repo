import os
import glob
from pathlib import Path
import shutil
import logging
import multiprocessing as mp
from obspy import read
from obspy.core.stream import Stream
from core.utils import date_range, get_sta_list

def equip_filter(args):
    day, station_list, output_base_dir, data_path = args
    logging.basicConfig(filename=output_base_dir / day / 'log' / f'data_single.log', level=logging.INFO, filemode='a')
    process_dir_path = output_base_dir / day / 'data' / 'data_single'
    process_dir_path.mkdir(parents=True, exist_ok=True)
    for sta in station_list:
        logging.info(f"We are in {sta}")
        station_sac_all = glob.glob(os.path.join(data_path, day, f"*{sta}*"))
        logging.info(f"station_sac_all:{station_sac_all}")
        #station_sac_all = (Path(data_path) / day).glob(f"*{sta}*")
        checkpoint = 0
        try:
            # HH
            for station_sac in station_sac_all:
                station_equip = os.path.basename(station_sac).split('.')[3][:2]
                logging.info(f'station_eauip:{station_equip}')
                if station_equip == 'HH':
                    checkpoint = 1  # record we have HH
                    shutil.copy(station_sac, process_dir_path)
                    logging.info(f"{sta} using HH")
                else:
                    logging.info(f"{sta} don't have HH")
            # BH
            for station_sac in station_sac_all:
                if checkpoint == 1:
                    break
                station_equip = os.path.basename(station_sac).split('.')[3][:2]
                logging.info(f'station_eauip:{station_equip}')
                if station_equip == 'BH':
                    checkpoint = 2
                    shutil.copy(station_sac, process_dir_path)
                    logging.info(f"{sta} using BH")
                else:
                    logging.info(f"{sta} don't have BH")
            # EH
            for station_sac in station_sac_all:
                if checkpoint in [1, 2]:
                    break
                station_equip = os.path.basename(station_sac).split('.')[3][:2]
                if station_equip == 'EH':
                    checkpoint = 3
                    shutil.copy(station_sac, process_dir_path)
                    logging.info(f"{sta} using EH")
                else:
                    logging.info(f"{sta} don't have EH")
            # EP
            for station_sac in station_sac_all:
                if checkpoint in [1, 2, 3]:
                    break
                station_equip = os.path.basename(station_sac).split('.')[3][:2]
                if station_equip == 'EP':
                    checkpoint = 4
                    shutil.copy(station_sac, process_dir_path)
                    logging.info(f"{sta} using EP")
                else:
                    logging.info(f"{sta} don't have EP")
            # HL
            for station_sac in station_sac_all:
                if checkpoint in [1, 2, 3, 4]:
                    break
                station_equip = os.path.basename(station_sac).split('.')[3][:2]
                if station_equip == 'HL':
                    checkpoint = 5
                    shutil.copy(station_sac, process_dir_path)
                    logging.info(f"{sta} using HL")
                else:
                    logging.info(f"{sta} don't have HL")
            # BL
            for station_sac in station_sac_all:
                if checkpoint in [1, 2, 3, 4, 5]:
                    break
                station_equip = os.path.basename(station_sac).split('.')[3][:2]
                if station_equip == 'BL':
                    checkpoint = 6
                    shutil.copy(station_sac, process_dir_path)
                    logging.info(f"{sta} using BL")
                else:
                    logging.info(f"{sta} don't have BL")
            # HN
            for station_sac in station_sac_all:
                if checkpoint in [1, 2, 3, 4, 5, 6]:
                    break
                station_equip = os.path.basename(station_sac).split('.')[3][:2]
                if station_equip == 'HN':
                    checkpoint = 7
                    shutil.copy(station_sac, process_dir_path)
                    logging.info(f"{sta} using HN")
                else:
                    logging.info(f"{sta} don't have HN")
        except Exception as e:
            logging.info(f"We don't have {sta} so error {e}")

def merging(args):
    date, station_list, output_base_dir = args
    logging.basicConfig(filename=output_base_dir / date / 'log' / f'data_final.log', level=logging.INFO, filemode='a')
    ori_data_dir = output_base_dir / date / 'data' / 'data_single' 
    output_data_dir = output_base_dir / date / 'data' / 'data_final'
    output_data_dir.mkdir(parents=True, exist_ok=True)
    for station in station_list:
        logging.info(f'we are in {station} now!')
        data_premerges = list(ori_data_dir.glob(f"*{station}*"))
        if not data_premerges:
            logging.info(f"we did not have {station} data, pass")
            continue

        equip_list = []
        numeral_list =[]
        for data_premerge in data_premerges:
            equip_extract = data_premerge.stem.split('.')[3] 
            if equip_extract not in equip_list:
                equip_list.append(equip_extract) 
            numerals = data_premerge.stem.split('.')[2]
            if numerals not in numeral_list:
                numeral_list.append(numerals)
        logging.info(f"equip list is {equip_list}")
        logging.info(f"numeral list is {numeral_list}")
        for equip in equip_list:
            for numeral in numeral_list:
                num_file = list(ori_data_dir.glob(f'*{station}.{numeral}.{equip}*'))
                logging.info(f'we got {numeral}.{equip} for {len(num_file)}')
                if len(num_file) > 1:
                    st_add = Stream()
                    for file_file in num_file:
                        st = read(file_file)
                        st_add += st
                    try:
                        st_add.merge(fill_value='interpolate')
                    except Exception as e:
                        logging.info(f"{e} happened in {numeral}.{equip}")
                    st_add.write(os.path.join(output_data_dir, os.path.basename(num_file[0])),format='SAC')
                    logging.info('save your tears for another day')
                elif len(num_file) == 0:
                    logging.info(f'No data in {numeral}.{equip}')
                    continue
                else:
                    logging.info('only U')
                    st_ori = read(num_file[0])
                    st_ori.write(os.path.join(output_data_dir, os.path.basename(num_file[0])),format='SAC')

class Initializer:
    def __init__(self, config):
        self.config = config
        self.data_path = config['waveform_dir']
        self.current_dir = Path.cwd()
        self.output_base_dir = self.current_dir / "output" / config['name_of_eq_sequence']
        self.date_list = date_range(config["analyze_range"].split('-')[0], config["analyze_range"].split('-')[1])
        self.station_path = config['station_path']
        if not config.get("pz_dir"):
            self.mag_run = False
        else:
            self.pz_path = config["pz_dir"]
            self.mag_run = True
        self.aso_method = config['association_method']
    def create_directory_structure(self):
        """
        Create the required directory structure based on the configuration.
        """
        self.output_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create directories for each date in the range
        for date in self.date_list:
            date_dir = self.output_base_dir / date
            date_dir.mkdir(parents=True, exist_ok=True)
            (date_dir / "log").mkdir(parents=True, exist_ok=True)
            (date_dir / "data").mkdir(parents=True, exist_ok=True)
            (date_dir / "phasenet").mkdir(parents=True, exist_ok=True)
            (date_dir / "h3dd").mkdir(parents=True, exist_ok=True)
            (date_dir / "Magnitude").mkdir(parents=True, exist_ok=True)
            (date_dir / "Focal").mkdir(parents=True, exist_ok=True)

            if self.config["association_method"] == "gamma":
                (date_dir / "GaMMA").mkdir(parents=True, exist_ok=True)
            elif self.config["association_method"] == "assoloc":
                (date_dir / "AssoLoc").mkdir(parents=True, exist_ok=True)
        
        # Copy stations.csv to the base output directory
        station_path = Path(self.config["station_path"])
        shutil.copy(station_path, self.output_base_dir / "stations.csv")
        
        # Create the 'Catelog' directory
        (self.output_base_dir / "Catelog").mkdir(parents=True, exist_ok=True)
    def filter_single_equip(self):
        days = self.date_list
        station_list = get_sta_list(self.station_path)
        args = [(day, station_list, self.output_base_dir, self.data_path) for day in days]
        cores = min(len(days), 10)
        with mp.Pool(processes=cores) as pool:
            pool.map(equip_filter, args)
    def merge_waveform(self):
        days = self.date_list
        station_list = get_sta_list(self.station_path)
        args = [(date, station_list, self.output_base_dir) for date in days]
        cores = min(len(days), 10)
        with mp.Pool(processes=cores) as pool:
            pool.map(merging, args)   
    def get_materials(self):
        parent_dir = str(self.output_base_dir)
        station_path = self.station_path 
        date_list = self.date_list
        mag_run = self.mag_run
        if mag_run:
            pz_dir = self.pz_path
        else:
            pz_dir = ""
        aso_method = self.aso_method
        return parent_dir, station_path, date_list, mag_run, pz_dir, aso_method