# NEEMQuery

A python library for easily querying [NEEMS](https://ease-crc.github.io/soma/owl/1.1.0/NEEM-Handbook.pdf) which have
been migrated to an SQL database using the [neem_to_sql](https://github.com/AbdelrhmanBassiouny/neem_to_sql) converter.
NEEMQuery uses SQLAlchemy and provides similar interface to the users. This makes if feel very familiar to SQLAlchemhy
or SQL users in general.

## Example Usage

Getting a robot plan for a certain neem:
Assumes the neems are located in a 'test' database at 'localhost' which can be accessed by 'newuser' using password '
password'.

```Python
from neem_query import NeemQuery, TaskType, ParticipantType
from neem_query.neems_database import *

nq = NeemQuery("mysql+pymysql://newuser:password@localhost/test")

neem_id = 2

na_query = (nq.select(TaskType.o.label('task')).
            select_time_columns().
            select_from(DulExecutesTask).
            join_task_types().
            join_neems().filter(Neem.ID == neem_id).
            join_task_time_interval().
            order_by(SomaHasIntervalBegin.o))

df = na_query.get_result()

print(df)
```

The result looks like this:

```
             task         begin           end
0  soma:PhysicalTask  1.608293e+09  1.608293e+09
1  soma:PhysicalTask  1.608293e+09  1.608293e+09
2      soma:Fetching  1.608293e+09  1.608293e+09
3      soma:Reaching  1.608293e+09  1.608293e+09
4     soma:PickingUp  1.608293e+09  1.608293e+09
5  soma:Transporting  1.608293e+09  1.608293e+09
6      soma:MovingTo  1.608293e+09  1.608293e+09
7       soma:Placing  1.608293e+09  1.608293e+09
8      soma:Lowering  1.608293e+09  1.608293e+09
9     soma:Releasing  1.608293e+09  1.608293e+09
```
