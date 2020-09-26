
module TestDataFrames

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "@transform" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    m = [100, 200, 300, 400, 500]

    gq = :g
    iq = :i
    tq = :t
    yq = :y
    cq = :c

    gr = "g"
    ir = "i"
    tr = "t"
    yr = "y"
    cr = "c"

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    @test @transform(df, n = :i).n == df.i
    @test @transform(df, n = :i .+ :g).n == df.i .+ df.g
    @test @transform(df, n = :t .* string.(:y)).n == df.t .* string.(df.y)
    @test @transform(df, n = Symbol.(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform(df, n = Symbol.(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform(df, body = :i).body == df.i
    @test @transform(df, transform = :i).transform == df.i

    @test @transform(df, n = cols(iq)).n == df.i
    @test @transform(df, n = cols(iq) .+ cols(gq)).n == df.i .+ df.g
    @test @transform(df, n = cols(tq) .* string.(cols(yq))).n == df.t .* string.(df.y)
    @test @transform(df, n = Symbol.(cols(yq), ^(:t))).n == Symbol.(df.y, :t)
    @test @transform(df, n = Symbol.(cols(yq), ^(:body))).n == Symbol.(df.y, :body)
    @test @transform(df, body = cols(iq)).body == df.i
    @test @transform(df, transform = cols(iq)).transform == df.i

    @test @transform(df, n = cols(ir)).n == df.i
    @test @transform(df, n = cols(ir) .+ cols(gr)).n == df.i .+ df.g
    @test @transform(df, n = cols(tr) .* string.(cols(yr))).n == df.t .* string.(df.y)
    @test @transform(df, n = Symbol.(cols(yr), ^(:t))).n == Symbol.(df.y, :t)
    @test @transform(df, n = Symbol.(cols(yr), ^(:body))).n == Symbol.(df.y, :body)
    @test @transform(df, body = cols(ir)).body == df.i
    @test @transform(df, transform = cols(ir)).transform == df.i

    @test @transform(df, n = :i).g !== df.g

    newdf = @transform(df, n = :i)
    @test newdf[:, Not(:n)] ≅ df

    @test @transform(df, :i) ≅ df
    @test @transform(df, :i, :g) ≅ df

    @test @transform(df, cols("new_column") = :i).new_column == df.i
    @test @transform(df, cols(n_str) = :i).new_column == df.i
    @test @transform(df, cols(n_sym) = :i).new_column == df.i
    @test @transform(df, cols(n_space) = :i)."new column" == df.i
    @test @transform(df, cols("new" * "_" * "column") = :i).new_column == df.i

    @test @transform(df, n = 1).n == fill(1, nrow(df))
end

# Defined outside of `@testset` due to use of `@eval`
df = DataFrame(
    g = [1, 1, 1, 2, 2],
    i = 1:5,
    t = ["a", "b", "c", "c", "e"],
    y = [:v, :w, :x, :y, :z],
    c = [:g, :quote, :body, :transform, missing]
    )

m = [100, 200, 300, 400, 500]

gq = :g
iq = :i
tq = :t
yq = :y
cq = :c

gr = "g"
ir = "i"
tr = "t"
yr = "y"
cr = "c"

s = [:i, :g]

@testset "limits of @transform" begin
    ## Test for not-implemented or strange behavior
    # @test_throws throws a `LoadError` when it
    # should throw an `ArgumentError`. Regardless,
    # the following should error so these tests are
    # left in.
    #
    # This behavior is part of `@test_throws` and
    # not part of DataFramesMeta.
    @test_throws LoadError @eval @transform(df, [:i, :g])
    @test_throws LoadError @eval @transform(df, All())
    @test_throws LoadError @eval @transform(df, Between(:i, :t)).Between == df.i
    @test_throws LoadError @eval @transform(df, Not(:i)).Not == df.i
    @test_throws LoadError @eval @transform(df, Not([:i, :g]))
    @test_throws MethodError @eval @transform(df, n = sum(Between(:i, :t)))
    @test_throws ArgumentError @eval @transform(df, n = sum(cols(s)))
end

@testset "@select" begin
    # Defined outside of `@testset` due to use of `@eval`
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    m = [100, 200, 300, 400, 500]

    gq = :g
    iq = :i
    tq = :t
    yq = :y
    cq = :c

    gr = "g"
    ir = "i"
    tr = "t"
    yr = "y"
    cr = "c"

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    @test @select(df, :i) == df[!, [:i]]
    @test @select(df, :i, :g) == df[!, [:i, :g]]
    df2 = copy(df)
    df2.n = df2.i .+ df2.g
    @test @select(df, :i, :g, n = :i .+ :g) == df2[!, [:i, :g, :n]]

    @test @select(df, n = :i).n == df.i
    @test @select(df, n = :i .+ :g).n == df.i .+ df.g
    @test @select(df, n = :t .* string.(:y)).n == df.t .* string.(df.y)
    @test @select(df, n = Symbol.(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @select(df, n = Symbol.(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @select(df, body = :i).body == df.i
    @test @select(df, transform = :i).transform == df.i

    @test @select(df, n = cols(iq)).n == df.i
    @test @select(df, n = cols(iq) .+ cols(gq)).n == df.i .+ df.g
    @test @select(df, n = cols(tq) .* string.(cols(yq))).n == df.t .* string.(df.y)
    @test @select(df, n = Symbol.(cols(yq), ^(:t))).n == Symbol.(df.y, :t)
    @test @select(df, n = Symbol.(cols(yq), ^(:body))).n == Symbol.(df.y, :body)
    @test @select(df, body = cols(iq)).body == df.i
    @test @select(df, transform = cols(iq)).transform == df.i

    @test @select(df, n = cols(ir)).n == df.i
    @test @select(df, n = cols(ir) .+ cols(gr)).n == df.i .+ df.g
    @test @select(df, n = cols(tr) .* string.(cols(yr))).n == df.t .* string.(df.y)
    @test @select(df, n = Symbol.(cols(yr), ^(:t))).n == Symbol.(df.y, :t)
    @test @select(df, n = Symbol.(cols(yr), ^(:body))).n == Symbol.(df.y, :body)
    @test @select(df, body = cols(ir)).body == df.i
    @test @select(df, transform = cols(ir)).transform == df.i

    @test DataFramesMeta.select(df, :i) == df.i

    @test @select(df, n = 1).n == fill(1, nrow(df))

    @test @select(df, cols("new_column") = :i).new_column == df.i
    @test @select(df, cols(n_str) = :i).new_column == df.i
    @test @select(df, cols(n_sym) = :i).new_column == df.i
    @test @select(df, cols(n_space) = :i)."new column" == df.i
    @test @select(df, cols("new" * "_" * "column") = :i).new_column == df.i

end

# Defined outside of `@testset` due to use of `@eval`
df = DataFrame(
    g = [1, 1, 1, 2, 2],
    i = 1:5,
    t = ["a", "b", "c", "c", "e"],
    y = [:v, :w, :x, :y, :z],
    c = [:g, :quote, :body, :transform, missing]
    )

m = [100, 200, 300, 400, 500]

gq = :g
iq = :i
tq = :t
yq = :y
cq = :c

gr = "g"
ir = "i"
tr = "t"
yr = "y"
cr = "c"

@testset "limits of @select" begin
    ## Test for not-implemented or strange behavior
    @test_throws LoadError @eval @select(df, [:i, :g])
    @test_throws LoadError @eval @select(df, All())
    @test_throws LoadError @eval @select(df, Between(:i, :t)).Between == df.i
    @test_throws LoadError @eval  @select(df, Not(:i)).Not == df.i
    @test_throws LoadError @eval @select(df, Not([:i, :g]))
    @test_throws MethodError @eval @select(df, n = sum(Between(:i, :t)))
    @test_throws ArgumentError @eval @select(df, n = sum(cols(s)))
end

@testset "Keyword arguments failure" begin
    @test_throws LoadError @eval @transform(df; n = :i)
    @test_throws LoadError @eval @select(df; n = :i)
end

@testset "with" begin
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
end

@testset "where" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])

    x = [2, 1, 0]

    @test DataFramesMeta.where(df, 1) == df[1, :]

    @test  @where(df, :A .> 1)          == df[df.A .> 1,:]
    @test  @where(df, :B .> 1)          == df[df.B .> 1,:]
    @test  @where(df, :A .> x)          == df[df.A .> x,:]
    @test  @where(df, :B .> x)          == df[df.B .> x,:]
    @test  @where(df, :A .> :B)         == df[df.A .> df.B,:]
    @test  @where(df, :A .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
    @test  @where(df, :A .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]
end


@test DataFramesMeta.orderby(df, df[[1, 3, 2], :]) == df[[1, 3, 2], :]

# `y` needs to be in global scope here because the testset relies on `y`
y = 0
@testset "byrow" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])

    @test @byrow(df, if :A > :B; :A = 0 end) == DataFrame(A = [1, 0, 0], B = [2, 1, 2])

    # No test for checking if the `@byrow!` deprecation warning exists because it
    # seems like Test.@test_logs (or Test.collect_test_logs) does not play nice
    # with macros.  The existence of the deprecation can be confirmed, however,
    # from the fact it appears a single time (because of the test below) when
    # `] test` is run.
    @test @byrow(df, if :A > :B; :A = 0 end) == @byrow!(df, if :A > :B; :A = 0 end)

    @test  df == DataFrame(A = [1, 2, 3], B = [2, 1, 2])

    df = DataFrame(A = 1:3, B = [2, 1, 2])  # Restore df
    @byrow(df, if :A + :B == 3; global y += 1 end)
    @test  y == 2

    df = DataFrame(A = 1:3, B = [2, 1, 2])
    df2 = @byrow df begin
        @newcol colX::Array{Float64}
        @newcol colY::Array{Float64}
        :colX = :B == 2 ? pi * :A : :B
        if :A > 1
            :colY = :A * :B
        end
    end

    @test  df2.colX == [pi, 1.0, 3pi]
    @test  df2[2, :colY] == 2
end

@testset "cols with @byrow" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])
    n = :A
    df2 = @byrow df begin
        :B = cols(n)
    end
    @test df2 == DataFrame(A = 1:3, B = 1:3)

    n = "A"
    df2 = @byrow df begin
        :B = cols(n)
    end
    @test df2 == DataFrame(A = 1:3, B = 1:3)

    n = 1
    df2 = @byrow df begin
        :B = cols(n)
    end
    @test df2 == DataFrame(A = 1:3, B = 1:3)

    n = :A
    df2 = @byrow df begin
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [2, 1, 2], B = [2, 1, 2])

    n = "A"
    df2 = @byrow df begin
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [2, 1, 2], B = [2, 1, 2])

    n = 1
    df2 = @byrow df begin
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [2, 1, 2], B = [2, 1, 2])


    n = :C
    df2 = @byrow df begin
        @newcol cols(n)::Vector{Int}
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [1, 2, 3], B = [2, 1, 2], C = [2, 1, 2])

    n = "C"
    df2 = @byrow df begin
        @newcol cols(n)::Vector{Int}
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [1, 2, 3], B = [2, 1, 2], C = [2, 1, 2])
end

df = DataFrame(A = 1:3, B = [2, 1, 2])

@testset "limits of @byrow" begin
    @eval TestDataFrames n = ["A", "B"]
    @test_throws ArgumentError @eval @byrow df begin cols(n) end

    @eval TestDataFrames n = [:A, :B]
    @test_throws ArgumentError @eval @byrow df begin cols(n) end

    @eval TestDataFrames n = [1, 2]
    @test_throws ArgumentError @eval @byrow df begin cols(n) end
end

end # module