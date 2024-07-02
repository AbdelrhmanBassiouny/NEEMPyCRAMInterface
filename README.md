# NEEMPyCRAMInterface

A python library for using the SQL NEEMs database (with the help of [NEEMQuery](https://github.com/AbdelrhmanBassiouny/NEEMQuery))
in combination with [PyCRAM](https://github.com/cram2/pycram) to be able to replay the NEEM motions or redo the executed plan 
in the NEEM using the PyCRAM framework.
## Installation


### MariaDB Installation
Make sure you have [MariaDB](https://www.digitalocean.com/community/tutorials/how-to-install-mariadb-on-ubuntu-20-04) installed on your device. 

### PyCRAM Installation
Also you need to have [PyCRAM](https://github.com/AbdelrhmanBassiouny/pycram/tree/multiverse_interface) installed on your device,
1. clone the PyCRAM repository, 
2. follow the [PyCRAM]() installation instructions 
3. checkout the 'multiverse_interface' branch.
```shell
git checkout multiverse_interface
```


```bash
pip install neem_pycram_interface
```

## Example Usage

All below examples assume the neems are located in a 'test' database at 'localhost' which can be accessed by 'newuser'
using password 'password'.

### Replaying the motions of multiple NEEMs:

This is done by using the PyCRAMNEEMInterface class which provides a higher level of abstraction,
and uses PyCRAM to replay the motions retrieved from the SQL queries. This shows the replay of three NEEMs in real time.

```Python
from neem_pycram_interface.neem_pycram_interface import PyCRAMNEEMInterface

from pycram.datastructures.enums import WorldMode
from pycram.worlds.bullet_world import BulletWorld
from pycram.ros.viz_marker_publisher import VizMarkerPublisher

pni = PyCRAMNEEMInterface('mysql+pymysql://newuser:password@localhost/test')
world = BulletWorld(mode=WorldMode.DIRECT)
vis_mark_publisher = VizMarkerPublisher()

neem_ids = [14, 15, 16]
pni.replay_motion_of_neem(neem_ids, real_time=False)
```

https://github.com/AbdelrhmanBassiouny/NEEMPyCRAMInterface/assets/36744004/d6179b69-6dc0-43bc-ac18-6fa237542d03

