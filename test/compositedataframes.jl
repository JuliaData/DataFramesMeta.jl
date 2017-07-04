
module TestCompositeDataFrames

using Base.Test
using DataArrays, DataFrames
using DataFramesMeta

df = CompositeDataFrame(A = [1, 2, 3], B = [2, 1, 2])
x = [2, 1, 0]

@test  df.A == df[:A]
@test  size(df[1:2,:]) == (2,2)
@test  size(df[[1, 2],:]) == (2,2)
@test  size(df[[1, 2], [:A]]) == (2,1)
@test  size(df[:, [:A]]) == (3,1)
@test  size(df[:, 1:2]) == (3,2)
@test  size(df[:, [1]]) == (3,1)
@test  size(df[df.A .< 3, :]) == (2,2)
@test  size([df df]) == (3,4)

@test  @with(df, :A + 1)   ==  df[:A] + 1
@test  @with(df, :A + :B)  ==  df[:A] + df[:B]
@test  @with(df, :A + x)   ==  df[:A] + x
x = @with df begin
    res = 0.0
    for i in 1:length(:A)
        res += :A[i] * :B[i]
    end
    res
end
@test  x == sum(df[:A] .* df[:B])
@test  @with(df, df[:A .> 1, ^([:B, :A])]) == df[df[:A] .> 1, [:B, :A]]
@test  @with(df, DataFrame(a = :A * 2, b = :A + :B)) == DataFrame(a = df[:A] * 2, b = df[:A] + df[:B])

@test  @where(df, :A .> 1)         == df[df[:A] .> 1,:]
@test  @where(df, :B .> 1)         == df[df[:B] .> 1,:]
@test  @where(df, :A .> x)         == df[df[:A] .> x,:]
@test  @where(df, :B .> x)         == df[df[:B] .> x,:]
@test  @where(df, :A .> :B)        == df[df[:A] .> df[:B],:]

@test @orderby(df, :B)[:A] == [2, 1, 3]

df2 = @transform(df, C = :A + 1)
@test df2.C == df.A + 1
@test df2[:C] == df2[:A] + 1

df3 = @select(df, :B, C = :A + 1, :A)
@test df3.C == df.A + 1

df = CompositeDataFrame(:MyDF, A = [1, 2, 3], B = [2, 1, 2])
@test typeof(df) == DataFramesMeta.MyDF
@test  df.A == df[:A]

res = 0
for x in eachrow(df)
    res += x.A * x.B
end
@test res == sum(df.A .* df.B)

itr = eachrow(df)
@test size(itr) == (3,)
@test length(itr) == 3
@test getindex(itr, 1) == row(itr.df, 1)
@test map(itr) do row
    row.A * row.B
end == [2, 2, 6]

df2 = DataFrame(C = [1, 2, 3], D = [2, 1, 2])
df3 = CompositeDataFrame(:DF3, C = [1, 2, 3], D = [2, 1, 2])

@test names(hcat(df, df2)) == [:A, :B, :C, :D]
@test names(hcat(df2, df)) == [:C, :D, :A, :B]
@test names(hcat(df, df3)) == [:A, :B, :C, :D]

@test CompositeDataFrame(DataFrame(df)) == df
@test names(CompositeDataFrame(DataFrame(df), [:C, :D])) == [:C, :D]
@test row() == nothing
@test df[1, :A] == 1
@test names( df[[1, 2]] ) == [:A, :B]
@test names( df[1:1] ) == [:A]

end # module
