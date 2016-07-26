# Julia benchmarks from R's data.table
# https://github.com/Rdatatable/data.table/wiki/Benchmarks-:-Grouping

using DataFrames, DataFramesMeta
using NullableArrays

N=10_000_000; K=100
srand(1)

# Array version

DA = DataFrame(
  id1 = P(rand([@compat(Symbol("id", i)) for i=1:K], N)),          # large groups (char)
  id2 = P(rand([@compat(Symbol("id", i)) for i=1:K], N)),          # large groups (char)
  id3 = P(rand([@compat(Symbol("id", i)) for i=1:N÷K], N)),        # small groups (char)
  id4 = P(rand(1:K, N)),                          # large groups (int)
  id5 = P(rand(1:K, N)),                          # large groups (int)
  id6 = P(rand(1:N÷K, N)),                        # small groups (int)
  v1 =  P(rand(1:5, N)),                          # int in range [1,5]
  v2 =  P(rand(1:5, N)),                          # int in range [1,5]
  v3 =  P(rand(N))                                # numeric e.g. 23.5749
);

# PooledDataArray version

DPDA = DataFrame(
  id1 = PooledDataArray(rand([@compat(Symbol("id", i)) for i=1:K], N)),     # large groups (char)
  id2 = PooledDataArray(rand([@compat(Symbol("id", i)) for i=1:K], N)),     # large groups (char)
  id3 = PooledDataArray(rand([@compat(Symbol("id", i)) for i=1:N÷K], N)),   # small groups (char)
  id4 = PooledDataArray(rand(1:K, N)),                          # large groups (int)
  id5 = PooledDataArray(rand(1:K, N)),                          # large groups (int)
  id6 = PooledDataArray(rand(1:N÷K, N)),                        # small groups (int)
  v1 =  P(rand(1:5, N)),                          # int in range [1,5]
  v2 =  P(rand(1:5, N)),                          # int in range [1,5]
  v3 =  P(rand(N))                                # numeric e.g. 23.5749
);

# DataArray version

DDA = DataFrame(
  id1 = (rand([@compat(Symbol("id", i)) for i=1:K], N)),          # large groups (char)
  id2 = (rand([@compat(Symbol("id", i)) for i=1:K], N)),          # large groups (char)
  id3 = (rand([@compat(Symbol("id", i)) for i=1:N÷K], N)),        # small groups (char)
  id4 = (rand(1:K, N)),                          # large groups (int)
  id5 = (rand(1:K, N)),                          # large groups (int)
  id6 = (rand(1:N÷K, N)),                        # small groups (int)
  v1 =  (rand(1:5, N)),                          # int in range [1,5]
  v2 =  (rand(1:5, N)),                          # int in range [1,5]
  v3 =  (rand(N))                                # numeric e.g. 23.5749
);

# NullableArray version

DNA = DataFrame(
  id1 = P(NullableArray(rand([@compat(Symbol("id", i)) for i=1:K], N))),          # large groups (char)
  id2 = P(NullableArray(rand([@compat(Symbol("id", i)) for i=1:K], N))),          # large groups (char)
  id3 = P(NullableArray(rand([@compat(Symbol("id", i)) for i=1:N÷K], N))),   # small groups (char)
  id4 = P(NullableArray(rand(1:K, N))),                          # large groups (int)
  id5 = P(NullableArray(rand(1:K, N))),                          # large groups (int)
  id6 = P(NullableArray(rand(1:N÷K, N))),                        # small groups (int)
  v1 =  P(NullableArray(rand(1:5, N))),                          # int in range [1,5]
  v2 =  P(NullableArray(rand(1:5, N))),                          # int in range [1,5]
  v3 =  P(NullableArray(rand(N)))                                # numeric e.g. 23.5749
);


function dt_timings(D)
    @time @by(D, :id1, sv =sum(:v1));
    @time @by(D, :id1, sv =sum(:v1));
    @time @by(D, [:id1, :id2], sv =sum(:v1));
    @time @by(D, [:id1, :id2], sv =sum(:v1));
    @time @by(D, :id3, sv = sum(:v1), mv3 = mean(:v3));
    @time @by(D, :id3, sv = sum(:v1), mv3 = mean(:v3));
    @time aggregate(D[[4,7:9;]], :id4, mean);
    @time aggregate(D[[4,7:9;]], :id4, mean);
    @time aggregate(D[[6,7:9;]], :id6, sum);
    @time aggregate(D[[6,7:9;]], :id6, sum);
    return
end

dt_timings(DA)
dt_timings(DPDA)
dt_timings(DNA)
dt_timings(DDA)

@profile @by(D, :id1, sv =sum(:v1));
