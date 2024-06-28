import subprocess
from core.initializer import Initializer

class H3dd(Initializer):
    def __init__(self, config):
        super().__init__(config)
        self.for_h3dd_dir = self.output_base_dir / 'GaMMA' / 'for_h3dd'
        self.h3dd_dir = self.output_base_dir / 'h3dd'
        self.h3dd_station = self.output_base_dir / 'h3dd_station_format'
    def h3dd_inp(self, index, cut_off_dist):
        with open(self.current_dir / 'h3dd.inp', 'w') as f:
            f.write("*1. input catalog data\n")
            f.write(f"{str(self.for_h3dd_dir)}/gamma_new_{index}.dat_ch | awk -F/ '{{print $NF}}'\n")
            f.write("*2. station information file\n")
            f.write(f"{self.h3dd_station}\n")
            f.write("*3. 3d velocity model\n")
            f.write(f"{self.vel_model_3d}\n")
            f.write("*4. weighting for p wave, s wave, and single event data\n")
            f.write("*   wp  ws  wsingle\n")
            f.write("    1.  1.   0.1\n")
            f.write("*5. a priori weighting for catalog data\n")
            f.write("*   0      1      2      3      4\n")
            f.write("    1.   0.75   0.50   0.25   0.0\n")
            f.write("*6. cut off distance for D-D method (km)\n")
            f.write(f"    {cut_off_dist}.\n")
            f.write("*7. inv (1=SVD 2=LSQR)\n")
            f.write("    2\n")
            f.write("*8. damping factor (Only work if inv=2)\n")
            f.write("    0.\n")
            f.write("*9. rmscut (sec)\n")
            f.write("    1.e-4\n")
            f.write("*10. maximum interation times\n")
            f.write("    5\n")
            f.write("*11. constrain factor\n")
            f.write("    0.\n")
            f.write("*12. joint inversion with single event method (1=yes 0=no)\n")
            f.write("    1\n")
            f.write("*13. consider elevation or not (1=yes 0=no)\n")
            f.write("    0\n")
    def count_for_h3dd_files(self):
        return len(list(self.for_h3dd_dir.glob(f"*gamma_new*")))
    def run_h3dd(self):
        with open(self.current_dir / 'h3dd.inp', 'r') as file:
            subprocess.run(['./h3dd'], stdin=file, text=True, capture_output=True)
    