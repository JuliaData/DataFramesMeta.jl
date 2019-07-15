
module TestDataFrames

using Test
using DataFrames
using DataFramesMeta

df = DataFrame(A = 1:3, B = [2, 1, 2])

x = [2, 1, 0]

@test  @with(df, :A .+ 1)   ==  df.A .+ 1
@test  @with(df, :A .+ :B)  ==  df.A .+ df.B
@test  @with(df, :A .+ x)   ==  df.A .+ x

x = @with df begin
    res = 0.0
    for i in 1:length(:A)
        res += :A[i] * :B[i]
    end
    res
end
idx = :A
@test  @with(df, cols(idx) .+ :B)  ==  df.A .+ df.B
idx2 = :B
@test  @with(df, cols(idx) .+ cols(idx2))  ==  df.A .+ df.B

@test  x == sum(df.A .* df.B)
@test  @with(df, df[:A .> 1, ^([:B, :A])]) == df[df.A .> 1, [:B, :A]]
@test  @with(df, DataFrame(a = :A * 2, b = :A .+ :B)) == DataFrame(a = df.A * 2, b = df.A .+ df.B)

@test DataFramesMeta.where(df, 1) == df[1, :]

@test  @where(df, :A .> 1)          == df[df.A .> 1,:]
@test  @where(df, :B .> 1)          == df[df.B .> 1,:]
@test  @where(df, :A .> x)          == df[df.A .> x,:]
@test  @where(df, :B .> x)          == df[df.B .> x,:]
@test  @where(df, :A .> :B)         == df[df.A .> df.B,:]
@test  @where(df, :A .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
@test  @where(df, :A .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]

@test DataFramesMeta.select(df, :A) == df.A

@test DataFramesMeta.orderby(df, df[[1, 3, 2], :]) == df[[1, 3, 2], :]

@test @byrow!(df, if :A > :B; :A = 0 end) == DataFrame(A = [1, 0, 0], B = [2, 1, 2])
@test  df == DataFrame(A = [1, 2, 3], B = [2, 1, 2])

df = DataFrame(A = 1:3, B = [2, 1, 2])  # Restore df
y = 0
@byrow!(df, if :A + :B == 3; global y += 1 end)
@test  y == 2

df = DataFrame(A = 1:3, B = [2, 1, 2])
df2 = @byrow! df begin
    @newcol colX::Array{Float64}
    @newcol colY::Array{Float64}
    :colX = :B == 2 ? pi * :A : :B
    if :A > 1
        :colY = :A * :B
    end
end

@test  df2.colX == [pi, 1.0, 3pi]
@test  df2[2, :colY] == 2

end # module
