
module TestDataFrames

using Base.Test
using DataArrays, DataFrames
using DataFramesMeta

df = DataFrame(A = 1:3, B = [2, 1, 2])
x = [2, 1, 0]

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

@test  @ix(df, :A .> 1)           == df[df[:A] .> 1,:]
@test  @ix(df, :B .> 1)           == df[df[:B] .> 1,:]
@test  @ix(df, :A .> x)           == df[df[:A] .> x,:]
@test  @ix(df, :B .> x)           == df[df[:B] .> x,:]
@test  @ix(df, :A .> :B)          == df[df[:A] .> df[:B],:]
@test  @ix(df, :A .> 1, [:B, :A]) == df[df[:A] .> 1, [:B, :A]]

@test  @where(df, :A .> 1)         == df[df[:A] .> 1,:]
@test  @where(df, :B .> 1)         == df[df[:B] .> 1,:]
@test  @where(df, :A .> x)         == df[df[:A] .> x,:]
@test  @where(df, :B .> x)         == df[df[:B] .> x,:]
@test  @where(df, :A .> :B)        == df[df[:A] .> df[:B],:]

@test @byrow!(df, if :A > :B; :A = 0 end) == DataFrame(A = [1, 0, 0], B = [2, 1, 2])
@test  df == DataFrame(A = [1, 0, 0], B = [2, 1, 2])

df = DataFrame(A = 1:3, B = [2, 1, 2])  # Restore df
y = 0
function f(x)
    @byrow!(df, if :A + :B == 3; x += 1 end)
    x
end
y = f(y)
@test  y == 2

# tests for ix! and where!
df = DataFrame(A = 1:3, B = [2, 1, 2])
df_copy = deepcopy(df)
@test @ix!(df_copy, :A .> 1) == df[df[:A] .> 1,:]
@test df_copy == df[df[:A] .> 1,:]

df_copy = deepcopy(df)
@test @where!(df_copy, :A .> 1) == df[df[:A] .> 1,:]
@test df_copy == df[df[:A] .> 1,:]

end # module
