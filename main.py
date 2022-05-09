from join import Join
import sys


method = sys.argv[1]
file_path1 = sys.argv[2]
file_path2 = sys.argv[3]
column_name = sys.argv[4]
try:
  join_type = sys.argv[5]
except IndexError:
  join_type = 'inner'

if method == 'join':
    join = Join(file_path1, file_path2, column_name, join_type)
    data1, data2 = join.get_data_and_set_join_type()
    joined_df, joined_columns = join.create_df(data1, data2)
    joined_data = join.get_action(data1, data2, joined_df, joined_columns)
    join.save_to_csv(joined_data)

    



