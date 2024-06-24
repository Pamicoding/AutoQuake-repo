import os
import h5py
import logging
import time
import multiprocessing
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
from functools import partial
from multiprocessing import set_start_method, Pool
from tqdm import tqdm
from data_reader import DataReader_mseed_array, DataReader_pred
from model import ModelConfig, UNet
from postprocess import (
    extract_picks,
    save_picks,
    save_picks_json,
    save_prob_h5,
)
from visulization import plot_waveform

tf.compat.v1.disable_eager_execution()
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)


class PhaseNet:
    def __init__(self, model_path, config):
        self.model_path = model_path
        self.config = config
        self.model = self.load_model()

    def load_model(self):
        if not os.path.exists(self.model_path):
            raise FileNotFoundError(f"Model file not found: {self.model_path}")
        return tf.keras.models.load_model(self.model_path)

    def preprocess_data(self, data):
        data = np.array(data)
        data = (data - np.mean(data)) / np.std(data)
        data = data.reshape((1, -1, 1))
        return data

    def predict(self, data):
        preprocessed_data = self.preprocess_data(data)
        predictions = self.model.predict(preprocessed_data)
        return predictions

    def save_results(self, predictions, output_path):
        output_path = Path(output_path)
        np.save(output_path, predictions)
        print(f"Predictions saved to {output_path}")

    def pred_fn(self, args, data_reader, figure_dir=None, prob_dir=None, log_dir=None):
        logging.info(f"we are in pred_fn function in phasenet.py")
        current_time = time.strftime("%y%m%d-%H%M%S")
        if log_dir is None:
            log_dir = os.path.join(args.log_dir, "pred", current_time)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        if (args.plot_figure == True) and (figure_dir is None):
            figure_dir = os.path.join(log_dir, "figures")
            if not os.path.exists(figure_dir):
                os.makedirs(figure_dir)
        if (args.save_prob == True) and (prob_dir is None):
            prob_dir = os.path.join(log_dir, "probs")
            if not os.path.exists(prob_dir):
                os.makedirs(prob_dir)
        if args.save_prob:
            h5 = h5py.File(os.path.join(args.result_dir, "result.h5"), "w", libver="latest")
            prob_h5 = h5.create_group("/prob")
        logging.info("Pred log: %s" % log_dir)
        logging.info("Dataset size: {}".format(data_reader.num_data))

        with tf.compat.v1.name_scope("Input_Batch"):
            if args.format == "mseed_array":
                batch_size = 1
            else:
                batch_size = args.batch_size
            dataset = data_reader.dataset(batch_size)
            batch = tf.compat.v1.data.make_one_shot_iterator(dataset).get_next()

        config = ModelConfig(X_shape=data_reader.X_shape)
        with open(os.path.join(log_dir, "config.log"), "w") as fp:
            fp.write("\n".join("%s: %s" % item for item in vars(config).items()))

        model = UNet(config=config, input_batch=batch, mode="pred")
        sess_config = tf.compat.v1.ConfigProto()
        sess_config.gpu_options.allow_growth = True

        with tf.compat.v1.Session(config=sess_config) as sess:
            saver = tf.compat.v1.train.Saver(tf.compat.v1.global_variables(), max_to_keep=5)
            init = tf.compat.v1.global_variables_initializer()
            sess.run(init)

            latest_check_point = tf.train.latest_checkpoint(args.model_dir)
            logging.info(f"restoring model {latest_check_point}")
            saver.restore(sess, latest_check_point)

            picks = []
            amps = [] if args.amplitude else None
            if args.plot_figure:
                set_start_method("spawn")
                pool = Pool(multiprocessing.cpu_count())

            for _ in tqdm(range(0, data_reader.num_data, batch_size), desc="Pred"):
                if args.amplitude:
                    pred_batch, X_batch, amp_batch, fname_batch, t0_batch, station_batch = sess.run(
                        [model.preds, batch[0], batch[1], batch[2], batch[3], batch[4]],
                        feed_dict={model.drop_rate: 0, model.is_training: False},
                    )
                else:
                    pred_batch, X_batch, fname_batch, t0_batch, station_batch = sess.run(
                        [model.preds, batch[0], batch[1], batch[2], batch[3]],
                        feed_dict={model.drop_rate: 0, model.is_training: False},
                    )

                waveforms = amp_batch if args.amplitude else None
                picks_ = extract_picks(
                    preds=pred_batch,
                    file_names=fname_batch,
                    station_ids=station_batch,
                    begin_times=t0_batch,
                    config=args,
                    waveforms=waveforms,
                    use_amplitude=args.amplitude,
                    dt=1.0 / args.sampling_rate,
                )

                picks.extend(picks_)

                if args.plot_figure:
                    if not (isinstance(fname_batch, np.ndarray) or isinstance(fname_batch, list)):
                        fname_batch = [fname_batch.decode().rstrip(".mseed") + "_" + x.decode() for x in station_batch]
                    else:
                        fname_batch = [x.decode() for x in fname_batch]
                    pool.starmap(
                        partial(plot_waveform, figure_dir=figure_dir),
                        zip(X_batch, pred_batch, fname_batch),
                    )

                if args.save_prob:
                    if not (isinstance(fname_batch, np.ndarray) or isinstance(fname_batch, list)):
                        fname_batch = [fname_batch.decode().rstrip(".mseed") + "_" + x.decode() for x in station_batch]
                    else:
                        fname_batch = [x.decode() for x in fname_batch]
                    save_prob_h5(pred_batch, fname_batch, prob_h5)

            if len(picks) > 0:
                df = pd.DataFrame(picks)
                base_columns = [
                    "station_id",
                    "begin_time",
                    "phase_index",
                    "phase_time",
                    "phase_score",
                    "phase_type",
                    "file_name",
                ]
                if args.amplitude:
                    base_columns.append("phase_amplitude")
                    base_columns.append("phase_amp")
                    df["phase_amp"] = df["phase_amplitude"]

                df = df[base_columns]
                df.to_csv(os.path.join(args.result_dir, args.result_fname + ".csv"), index=False)

                print(
                    f"Done with {len(df[df['phase_type'] == 'P'])} P-picks and {len(df[df['phase_type'] == 'S'])} S-picks"
                )
            else:
                print(f"Done with 0 P-picks and 0 S-picks")
        return 0

    def run_prediction(self, args):
        logging.basicConfig(filename=os.path.join(args.result_dir, 'predict.log'), format="%(asctime)s %(message)s", level=logging.INFO)

        with tf.compat.v1.name_scope("create_inputs"):
            if args.format == "mseed_array":
                data_reader = DataReader_mseed_array(
                    data_dir=args.data_dir,
                    data_list=args.data_list,
                    stations=args.stations,
                    amplitude=args.amplitude,
                    highpass_filter=args.highpass_filter,
                )
            else:
                data_reader = DataReader_pred(
                    format=args.format,
                    data_dir=args.data_dir,
                    data_list=args.data_list,
                    hdf5_file=args.hdf5_file,
                    hdf5_group=args.hdf5_group,
                    amplitude=args.amplitude,
                    highpass_filter=args.highpass_filter,
                    response_xml=args.response_xml,
                    sampling_rate=args.sampling_rate,
                )

            self.pred_fn(args, data_reader, log_dir=args.result_dir)

        return
