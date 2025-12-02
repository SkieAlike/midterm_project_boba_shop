import psycopg2
from pgbenchmark import Benchmark

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="gvantsa",
    host="localhost",
    port="5433"
)

benchmark = Benchmark(db_connection=conn, number_of_runs=100)
benchmark.set_sql("SELECT device, "
       "count(session_id) "
"FROM sessions "
"where session_start between '2020-01-01' AND '2025-04-01' "
"GROUP BY device;"
)



for result in benchmark:
    # {'run': X, 'sent_at': <DATETIME WITH MS>, 'duration': '0.000064'}
    pass

""" View Summary """
print(benchmark.get_execution_results())

# {'runs': 1000,
#      'min_time': '0.000576',
#      'max_time': '0.014741',
#      'avg_time': '0.0007',
#      'median_time': '0.000642',
#      'percentiles': {'p25': '0.000612',
#                      'p50': '0.000642',
#                      'p75': '0.000696',
#                      'p99': '0.001331'}
#      }