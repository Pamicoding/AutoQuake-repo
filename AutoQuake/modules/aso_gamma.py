#%%
import os
import csv
import json
import logging
import multiprocessing as mp
from datetime import datetime
import pandas as pd
import numpy as np
from pyproj import Proj
from core.initializer import Initializer
from gamma.utils import association, estimate_eps

def extract_substring(s):
    parts = s.split('.')
    return parts[1]

def config2csv(config, filename='config_detailed'):
    with open(f'{filename}.csv', 'w') as f:
        for key, value in config.items():
            f.write(f'{key},{value}\n')

def convert_utc_datetime(utc_datetime_str):
    """
    Converts a UTC datetime string into a datetime object.
    Modify this function based on the format of your UTCDateTime strings.
    """
    return datetime.strptime(utc_datetime_str, '%Y-%m-%dT%H:%M:%S.%f')

def gamma_reorder(ori_csv, reorder_csv):
    # Open the input CSV file and read its contents
    with open(ori_csv, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        data = list(reader)
    data.sort(key=lambda row: convert_utc_datetime(row[0]))
    # Write the sorted data to a new CSV file
    with open(reorder_csv, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header) 
        writer.writerows(data) 

def gamma_chunk_split(split_dir, reorder_csv):
    split_dir.mkdir(parents=True, exist_ok=True)
    for i, chunk in enumerate(pd.read_csv(reorder_csv, chunksize=4000)):
        chunk.to_csv(split_dir / f'gamma_events_{i}.csv',index=False)

def transform(args):
    index, picks, split_dir, output_dir = args
    logging.basicConfig(filename='trans.log',level=logging.INFO,filemode='a')
    gamma_picks = picks
    gamma_events = os.path.join(split_dir, f'gamma_events_{index}.csv')
    logging.info(f'we are in gamma_events_{index}') 
    with open(gamma_events,'r') as f:
        lines = f.readlines()
        for line in lines:
            if line[0] != 't':
                item = line.split(',')
                utc_time = datetime.strptime(item[0], '%Y-%m-%dT%H:%M:%S.%f')
                ymd = utc_time.strftime('%Y%m%d')
                hh = utc_time.hour
                mm = utc_time.minute
                ss = round(utc_time.second + utc_time.microsecond / 1000000, 2)
                lon_int = int(float(item[-3]))
                lon_deg = (float(item[-3]) - lon_int)*60
                lat_int = int(float(item[-2]))
                lat_deg = (float(item[-2]) - lat_int)*60
                depth = round(float(item[-1]),2)
                event_index = item[9]
                output_file = output_dir / f'gamma_events_{index}.dat_ch'
                with open(output_file,'a') as r:
                    r.write(f"{ymd:>9}{hh:>2}{mm:>2}{ss:>6.2f}{lat_int:2}{lat_deg:0>5.2f}{lon_int:3}{lon_deg:0>5.2f}{depth:>6.2f}\n")
                with open(gamma_picks,'r') as picks_read:
                    picks_lines = picks_read.readlines()
                    for p_line in picks_lines:
                        if p_line[0] != 's':
                            picks_index = p_line.split(',')[-2]
                            if event_index == picks_index:
                                part = p_line.split(',')
                                wt = part[3]
                                sta = part[0].split('.')[1]
                                pick_time = datetime.strptime(part[1], '%Y-%m-%dT%H:%M:%S.%f')
                                if mm == 59 and pick_time.minute == 0: # modify
                                    wmm = int(60)
                                else:
                                    wmm = pick_time.minute
                                wss = round(pick_time.second + pick_time.microsecond / 1000000, 2)
                                wei = '1.00'
                                with open(output_file,'a') as r:
                                    if wt == 'P':
                                        r.write(f"{' ':1}{sta:<4}{'0.0':>6}{'0':>4}{'0':>4}{wmm:>4}{wss:>6.2f}{'0.01':>5}{wei:>5}{'0.00':>6}{'0.00':>5}{'0.00':>5}\n")
                                    else:
                                        r.write(f"{' ':1}{sta:<4}{'0.0':>6}{'0':>4}{'0':>4}{wmm:>4}{'0.00':>6}{'0.00':>5}{'0.00':>5}{wss:>6.2f}{'0.01':>5}{wei:>5}\n")
    logging.info(f'gamma_event_{index} transform is done')

class Aso_gamma(Initializer):
    def __init__(self, config, picks):
        super().__init__(config)
        self.picks =picks
        self.gamma_events = self.output_base_dir / 'GaMMA' / 'gamma_events.csv'
        self.gamma_order = self.output_base_dir / 'GaMMA' / 'gamma_events_order.csv'
        self.gamma_picks = self.output_base_dir / 'GaMMA' / 'gamma_picks.csv'
        self.split_dir = self.output_base_dir / 'GaMMA' / 'split_dir'
        self.for_h3dd = self.output_base_dir / 'GaMMA' / 'for_h3dd'
        self.for_h3dd.mkdir(parents=True, exist_ok=True)
    def run_gamma_association(self):
        region = self.output_base_dir / 'GaMMA'
        station_csv = self.output_base_dir / "stations.csv"
            
        pick_df = []
        for p in self.picks:
            pick_df.append({
                "id": p.trace_id,
                "timestamp": p.peak_time.datetime,
                "prob": p.peak_value,
                "type": p.phase.lower()
            })
        pick_df = pd.DataFrame(pick_df)
        pick_df['id'] = pick_df['id'].apply(extract_substring)

        col_to_keep = ['station', 'lon', 'lat', 'elevation_m']
        stations = pd.read_csv(station_csv, usecols=col_to_keep)
        stations.rename(columns={"station": "id"}, inplace=True)
        
        #
        config = {}
        x0 = 121.7
        y0 = 24.0
        xmin = 121.2
        xmax = 122.2
        ymin = 23.5
        ymax = 24.5
        config["center"] = (x0, y0)
        config["xlim_degree"] = (2 * xmin - x0, 2 * xmax - x0)
        config["ylim_degree"] = (2 * ymin - y0, 2 * ymax - y0)

        proj = Proj(f"+proj=sterea +lon_0={config['center'][0]} +lat_0={config['center'][1]} +units=km")
        stations[["x(km)", "y(km)"]] = stations.apply(lambda x: pd.Series(proj(longitude=x.longitude, latitude=x.latitude)), axis=1)
        stations["z(km)"] = stations["elevation_m"].apply(lambda x: -x/1e3)

        config["use_dbscan"] = True
        config["use_amplitude"] = False

        config["method"] = "BGMM"  
        if config["method"] == "BGMM":
            config["oversample_factor"] = 5
        if config["method"] == "GMM":
            config["oversample_factor"] = 1

        config["vel"] = {"p": 6.0, "s": 6.0 / 1.75}
        config["dims"] = ['x(km)', 'y(km)', 'z(km)']
        config["x(km)"] = proj(longitude=config["xlim_degree"], latitude=[config["center"][1]] * 2)[0]
        config["y(km)"] = proj(longitude=[config["center"][0]] * 2, latitude=config["ylim_degree"])[1]
        config["z(km)"] = (0, 60)
        config["bfgs_bounds"] = (
            (config["x(km)"][0] - 1, config["x(km)"][1] + 1),  
            (config["y(km)"][0] - 1, config["y(km)"][1] + 1),  
            (0, config["z(km)"][1] + 1),  
            (None, None),  
        )

        config["dbscan_eps"] = estimate_eps(stations, config["vel"]["p"]) 
        config["dbscan_min_samples"] = 3

        velocity_model = pd.read_csv(self.vel_model_1d, names=["zz", "vp", "vs"])
        velocity_model = velocity_model[velocity_model["zz"] <= config["z(km)"][1]]
        vel = {"z": velocity_model["zz"].values, "p": velocity_model["vp"].values, "s": velocity_model["vs"].values}
        h = 1.0
        config["eikonal"] = {"vel": vel, "h": h, "xlim": config["x(km)"], "ylim": config["y(km)"], "zlim": config["z(km)"]}

        config["ncpu"] = 35

        config["min_picks_per_eq"] = 8
        config["min_p_picks_per_eq"] = 6
        config["min_s_picks_per_eq"] = 2
        config["max_sigma11"] = 1.5 
        config["max_sigma22"] = 1.0 
        config["max_sigma12"] = 1.0 
        #
        if config["use_amplitude"]:
            picks = pick_df[pick_df["amp"] != -1]

        for k, v in config.items():
            print(f"{k}: {v}")
        config2csv(config, filename=region / 'config')

        event_idx0 = 0 
        assignments = []
        events, assignments = association(picks, stations, config, event_idx0, config["method"])
        event_idx0 += len(events)

        events = pd.DataFrame(events)
        events[["longitude","latitude"]] = events.apply(lambda x: pd.Series(proj(longitude=x["x(km)"], latitude=x["y(km)"], inverse=True)), axis=1)
        events["depth_km"] = events["z(km)"]
        events.to_csv(region / "gamma_events.csv", index=False, 
                        float_format="%.3f",
                        date_format='%Y-%m-%dT%H:%M:%S.%f')

        assignments = pd.DataFrame(assignments, columns=["pick_index", "event_index", "gamma_score"])
        picks = picks.join(assignments.set_index("pick_index")).fillna(-1).astype({'event_index': int})
        picks.rename(columns={"id": "station_id", "timestamp": "phase_time", "type": "phase_type", "prob": "phase_score", "amp": "phase_amplitude"}, inplace=True)
        picks.to_csv(region / "gamma_picks.csv", index=False, 
                        date_format='%Y-%m-%dT%H:%M:%S.%f')
    def gamma2h3dd(self):
        # order the event by date
        gamma_reorder(self.gamma_events, self.gamma_order)
        # split gamma_event into chunksize=4000 for running the h3dd
        gamma_chunk_split(self.split_dir, self.gamma_order)

        # transform the format
        chunk_num = len(os.listdir(str(self.split_dir))) # Path object needs to transform in string.
        index_list = np.arange(0, chunk_num)
        args = [(index, self.gamma_picks, self.split_dir, self.for_h3dd) for index in index_list]
        cores = min(chunk_num, 10)
        with mp.Pool(processes=cores) as pool:
            pool.map(transform, args)
        