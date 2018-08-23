# Julia benchmarks from R's data.table
# https://github.com/Rdatatable/data.table/wiki/Benchmarks-:-Grouping

using Random
using DataFrames, DataFramesMeta
using CategoricalArrays

N=10_0000; K=100
Random.seed!(1)

# Array version

DA = DataFrame(
  id1 = rand([Symbol("id", i) for i=1:K], N),          # large groups (char)
  id2 = rand([Symbol("id", i) for i=1:K], N),          # large groups (char)
  id3 = rand([Symbol("id", i) for i=1:N÷K], N),        # small groups (char)
  id4 = rand(1:K, N),                          # large groups (int)
  id5 = rand(1:K, N),                          # large groups (int)
  id6 = rand(1:N÷K, N),                        # small groups (int)
  v1 =  rand(1:5, N),                          # int in range [1,5]
  v2 =  rand(1:5, N),                          # int in range [1,5]
  v3 =  rand(N)                                # numeric e.g. 23.5749
);

# CategoricalArray version

DCA = DataFrame(
  id1 = CategoricalArray(rand([Symbol("id", i) for i=1:K], N)),     # large groups (char)
  id2 = CategoricalArray(rand([Symbol("id", i) for i=1:K], N)),     # large groups (char)
  id3 = CategoricalArray(rand([Symbol("id", i) for i=1:N÷K], N)),   # small groups (char)
  id4 = CategoricalArray(rand(1:K, N)),                          # large groups (int)
  id5 = CategoricalArray(rand(1:K, N)),                          # large groups (int)
  id6 = CategoricalArray(rand(1:N÷K, N)),                        # small groups (int)
  v1 =  rand(1:5, N),                          # int in range [1,5]
  v2 =  rand(1:5, N),                          # int in range [1,5]
  v3 =  rand(N)                                # numeric e.g. 23.5749
);

# Array{Union{T, Missing}} version

DMA = DataFrame(
  id1 = Array{Union{Symbol, Missing}}(rand([Symbol("id", i) for i=1:K], N)),          # large groups (char)
  id2 = Array{Union{Symbol, Missing}}(rand([Symbol("id", i) for i=1:K], N)),          # large groups (char)
  id3 = Array{Union{Symbol, Missing}}(rand([Symbol("id", i) for i=1:N÷K], N)),        # small groups (char)
  id4 = Array{Union{Int, Missing}}(rand(1:K, N)),                          # large groups (int)
  id5 = Array{Union{Int, Missing}}(rand(1:K, N)),                          # large groups (int)
  id6 = Array{Union{Int, Missing}}(rand(1:N÷K, N)),                        # small groups (int)
  v1 =  Array{Union{Int, Missing}}(rand(1:5, N)),                          # int in range [1,5]
  v2 =  Array{Union{Int, Missing}}(rand(1:5, N)),                          # int in range [1,5]
  v3 =  Array{Union{Float64, Missing}}(rand(N))                            # numeric e.g. 23.5749
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
dt_timings(DCA)
dt_timings(DMA)

@profile @by(DA, :id1, sv =sum(:v1));
