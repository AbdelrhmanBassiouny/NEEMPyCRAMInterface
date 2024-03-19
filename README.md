# NEEMQuery

A python library for easily querying [NEEMS](https://ease-crc.github.io/soma/owl/1.1.0/NEEM-Handbook.pdf) which have
been migrated to an SQL database using the [neem_to_sql](https://github.com/AbdelrhmanBassiouny/neem_to_sql) converter.
NEEMQuery uses SQLAlchemy and provides similar interface to the users. This makes if feel very familiar to SQLAlchemhy
or SQL users in general.

## Example Usage

All below examples assume the neems are located in a 'test' database at 'localhost' which can be accessed by 'newuser'
using password 'password'.

### Getting a robot plan for a certain neem (Highest Abstraction Level):

This is done by using the NeemInterface class which provides a higher level of abstraction and hides the complexity of
the underlying SQL queries.

```Python
from neem_query.neem_interface import NeemInterface

ni = NeemInterface("mysql+pymysql://newuser:password@localhost/test")

neem_id = 2
df = ni.get_task_sequence_of_neem(neem_id).get_result().df

print(df)
```

### Getting a robot plan for a certain neem using (Medium Abstraction Level):

This is done by using the NeemQuery class directly and using predefined joins and filters to build the query.

```Python
from neem_query import NeemQuery

nq = NeemQuery("mysql+pymysql://newuser:password@localhost/test")

neem_id = 2

df = (nq.select_task_type().
      select_time_columns().
      select_neem_id().
      select_from_tasks().
      join_task_types().
      join_neems().filter_by_neem_id(neem_id).
      join_task_time_interval().
      order_by_interval_begin()).get_result().df

print(df)
```

### Getting a robot plan for a certain neem (Lowest Abstraction Level):

This is done by using the NeemQuery class directly and explicitly writing the SQL query with the correct join conditions
and column names and table names.

```Python
from neem_query import NeemQuery, TaskType, ParticipantType
from neem_query.neems_database import *
from sqlalchemy import and_

nq = NeemQuery("mysql+pymysql://newuser:password@localhost/test")

neem_id = 2

df = (nq.select(TaskType.o).
      select(SomaHasIntervalBegin.o).select(SomaHasIntervalEnd.o).
      select(DulExecutesTask.neem_id).
      select_from(DulExecutesTask).
      join(TaskType,
           and_(TaskType.s == DulExecutesTask.dul_Task_o,
                TaskType.neem_id == DulExecutesTask.neem_id,
                TaskType.o != "owl:NamedIndividual")).
      join(Neem,
           Neem._id == DulExecutesTask.neem_id).filter(Neem.ID == neem_id).
      join(DulHasTimeInterval,
           and_(DulHasTimeInterval.dul_Event_s == DulExecutesTask.dul_Action_s,
                DulHasTimeInterval.neem_id == DulExecutesTask.neem_id)).
      join(SomaHasIntervalBegin,
           and_(SomaHasIntervalBegin.dul_TimeInterval_s == DulHasTimeInterval.dul_TimeInterval_o,
                SomaHasIntervalBegin.neem_id == DulHasTimeInterval.neem_id)).
      join(SomaHasIntervalEnd,
           and_(SomaHasIntervalEnd.dul_TimeInterval_s == DulHasTimeInterval.dul_TimeInterval_o,
                SomaHasIntervalEnd.neem_id == DulHasTimeInterval.neem_id)).
      order_by(SomaHasIntervalBegin.o)).get_result().df

print(df)
```

The result for all of the above examples is exactly the same,
and it looks like this:

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
