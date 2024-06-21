# AutoQuake-repo      
## Run
```
python AutoQuake/main.py --config=config.json
```
## Configuration example     
```
# station_path (str)
/home/patrick/Work/AutoQuake-repo/station.csv

# name_of_eq_sequence (str)
Hualien0403

# analyze_range (str)
20231230-20240102

# waveform_dir (str, data should in the daily)
/home/patrick/Work/dataset
├── 20240401
│   ├── TW.SM01.00.EPZ.D.2024.092
│   └── TW.SM01.00.HLZ.D.2024.092
│
├── 20240402
    ├── TW.SM01.00.EPZ.D.2024.093
    └── TW.SM01.00.HLZ.D.2024.093
```

