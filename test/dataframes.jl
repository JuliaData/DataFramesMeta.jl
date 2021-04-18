
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
    @test @transform(df, n = cols("g") + cols(:i)).n == df.g + df.i
    @test @transform(df, n = cols(1) + cols(2)).n == df.g + df.i

    @test @transform(df, n = @byrow :i).n == df.i
    @test @transform(df, n = @byrow :i + :g).n == df.i .+ df.g
    @test @transform(df, n = @byrow :t * string(:y)).n == df.t .* string.(df.y)
    @test @transform(df, n = @byrow Symbol(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform(df, n = @byrow Symbol(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform(df, body = @byrow :i).body == df.i
    @test @transform(df, transform = @byrow :i).transform == df.i
    @test @transform(df, n = @byrow :g == 1 ? 100 : 500).n == [100, 100, 100, 500, 500]

    @test @transform(df, n = :i).g !== df.g

    newdf = @transform(df, n = :i)
    @test newdf[:, Not(:n)] ≅ df

    @test @transform(df, :i) ≅ df
    @test @transform(df, :i, :g) ≅ df

    @test @transform(df, cols("new_column") = :i).new_column == df.i
    @test @transform(df, cols(n_str) = :i).new_column == df.i
    @test @transform(df, cols(n_str) = cols("i") .+ 0).new_column == df.i
    @test @transform(df, cols(n_sym) = :i).new_column == df.i
    @test @transform(df, cols(n_space) = :i)."new column" == df.i
    @test @transform(df, cols("new" * "_" * "column") = :i).new_column == df.i

    @test @transform(df, n = 1).n == fill(1, nrow(df))

    @test @transform(df, n = :i .* :g).n == [1, 2, 3, 8, 10]

end

@testset "@transform!" begin
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

    @test @transform!(df, n = :i).n == df.i
    @test @transform!(df, n = :i .+ :g).n == df.i .+ df.g
    @test @transform!(df, n = :t .* string.(:y)).n == df.t .* string.(df.y)
    @test @transform!(df, n = Symbol.(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform!(df, n = Symbol.(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform!(df, body = :i).body == df.i
    @test @transform!(df, transform = :i).transform == df.i

    @test @transform!(df, n = cols(iq)).n == df.i
    @test @transform!(df, n = cols(iq) .+ cols(gq)).n == df.i .+ df.g
    @test @transform!(df, n = cols(tq) .* string.(cols(yq))).n == df.t .* string.(df.y)
    @test @transform!(df, n = Symbol.(cols(yq), ^(:t))).n == Symbol.(df.y, :t)
    @test @transform!(df, n = Symbol.(cols(yq), ^(:body))).n == Symbol.(df.y, :body)
    @test @transform!(df, body = cols(iq)).body == df.i
    @test @transform!(df, transform = cols(iq)).transform == df.i

    @test @transform!(df, n = cols(ir)).n == df.i
    @test @transform!(df, n = cols(ir) .+ cols(gr)).n == df.i .+ df.g
    @test @transform!(df, n = cols(tr) .* string.(cols(yr))).n == df.t .* string.(df.y)
    @test @transform!(df, n = Symbol.(cols(yr), ^(:t))).n == Symbol.(df.y, :t)
    @test @transform!(df, n = Symbol.(cols(yr), ^(:body))).n == Symbol.(df.y, :body)
    @test @transform!(df, body = cols(ir)).body == df.i
    @test @transform!(df, transform = cols(ir)).transform == df.i
    @test @transform!(df, n = cols("g") + cols(:i)).n == df.g + df.i
    @test @transform!(df, n = cols(1) + cols(2)).n == df.g + df.i

    @test @transform!(df, cols("new_column") = :i).new_column == df.i
    @test @transform!(df, cols(n_str) = :i).new_column == df.i
    @test @transform(df, cols(n_str) = cols("i") .+ 0).new_column == df.i
    @test @transform!(df, cols(n_sym) = :i).new_column == df.i
    @test @transform!(df, cols(n_space) = :i)."new column" == df.i
    @test @transform!(df, cols("new" * "_" * "column") = :i).new_column == df.i

    @test @transform!(df, n = @byrow :i).n == df.i
    @test @transform!(df, n = @byrow :i + :g).n == df.i .+ df.g
    @test @transform!(df, n = @byrow :t * string(:y)).n == df.t .* string.(df.y)
    @test @transform!(df, n = @byrow Symbol(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform!(df, n = @byrow Symbol(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform!(df, body = @byrow :i).body == df.i
    @test @transform!(df, transform = @byrow :i).transform == df.i
    @test @transform!(df, n = @byrow :g == 1 ? 100 : 500).n == [100, 100, 100, 500, 500]

    @test @transform!(df, n = 1).n == fill(1, nrow(df))
    @test @transform!(df, n = :i .* :g).n == [1, 2, 3, 8, 10]

    # non-copying
    @test @transform!(df, n = :i).g === df.g
    @test @transform!(df, n = :i).n === df.i
    # mutating
    df2 = copy(df)
    @test @transform!(df, :i) === df
    @test df ≅ df2
    @test @transform!(df, :i, :g) ≅ df2
    @transform!(df, n2 = :i)
    @test df[:, Not(:n2)] ≅ df2
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
    @test_throws ArgumentError @eval @transform(df, y = :i + cols(1))
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
    @test @select(df, n = cols("g") + cols(:i)).n == df.g + df.i
    @test @select(df, n = cols(1) + cols(2)).n == df.g + df.i

    @test @select(df, n = @byrow :i).n == df.i
    @test @select(df, n = @byrow :i + :g).n == df.i .+ df.g
    @test @select(df, n = @byrow :t * string(:y)).n == df.t .* string.(df.y)
    @test @select(df, n = @byrow Symbol(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @select(df, n = @byrow Symbol(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @select(df, body = @byrow :i).body == df.i
    @test @select(df, transform = @byrow :i).transform == df.i
    @test @select(df, n = @byrow :g == 1 ? 100 : 500).n == [100, 100, 100, 500, 500]

    @test @select(df, n = 1).n == fill(1, nrow(df))

    @test @select(df, cols("new_column") = :i).new_column == df.i
    @test @select(df, cols(n_str) = :i).new_column == df.i
    @test @select(df, cols(n_str) = cols("i") .+ 0).new_column == df.i
    @test @select(df, cols(n_sym) = :i).new_column == df.i
    @test @select(df, cols(n_space) = :i)."new column" == df.i
    @test @select(df, cols("new" * "_" * "column") = :i).new_column == df.i

    @test @transform(df, n = :i .* :g).n == [1, 2, 3, 8, 10]

end

@testset "@select!" begin
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

    df2 = copy(df)
    df2.n = df2.i .+ df2.g

    @test @select!(copy(df), :i, :g, n = :i .+ :g) == df2[!, [:i, :g, :n]]
    @test @select!(copy(df), :i, :g) == df2[!, [:i, :g]]
    @test @select!(copy(df), :i) == df2[!, [:i]]

    @test @select!(copy(df), n = :i .+ :g).n == df.i .+ df.g
    @test @select!(copy(df), n = :i).n == df.i
    @test @select!(copy(df), n = :t .* string.(:y)).n == df.t .* string.(df.y)
    @test @select!(copy(df), n = Symbol.(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @select!(copy(df), n = Symbol.(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @select!(copy(df), body = :i).body == df.i
    @test @select!(copy(df), transform = :i).transform == df.i

    @test @select!(copy(df), n = cols(iq)).n == df.i
    @test @select!(copy(df), n = cols(iq) .+ cols(gq)).n == df.i .+ df.g
    @test @select!(copy(df), n = cols(tq) .* string.(cols(yq))).n == df.t .* string.(df.y)
    @test @select!(copy(df), n = Symbol.(cols(yq), ^(:t))).n == Symbol.(df.y, :t)
    @test @select!(copy(df), n = Symbol.(cols(yq), ^(:body))).n == Symbol.(df.y, :body)
    @test @select!(copy(df), body = cols(iq)).body == df.i
    @test @select!(copy(df), transform = cols(iq)).transform == df.i

    @test @select!(copy(df), n = cols(ir)).n == df.i
    @test @select!(copy(df), n = cols(ir) .+ cols(gr)).n == df.i .+ df.g
    @test @select!(copy(df), n = cols(tr) .* string.(cols(yr))).n == df.t .* string.(df.y)
    @test @select!(copy(df), n = Symbol.(cols(yr), ^(:t))).n == Symbol.(df.y, :t)
    @test @select!(copy(df), n = Symbol.(cols(yr), ^(:body))).n == Symbol.(df.y, :body)
    @test @select!(copy(df), body = cols(ir)).body == df.i
    @test @select!(copy(df), transform = cols(ir)).transform == df.i
    @test @select!(copy(df), n = cols("g") + cols(:i)).n == df.g + df.i
    @test @select!(copy(df), n = cols(1) + cols(2)).n == df.g + df.i

    @test @select!(copy(df), n = @byrow :i).n == df.i
    @test @select!(copy(df), n = @byrow :i + :g).n == df.i .+ df.g
    @test @select!(copy(df), n = @byrow :t * string(:y)).n == df.t .* string.(df.y)
    @test @select!(copy(df), n = @byrow Symbol(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @select!(copy(df), n = @byrow Symbol(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @select!(copy(df), body = @byrow :i).body == df.i
    @test @select!(copy(df), transform = @byrow :i).transform == df.i
    @test @select!(copy(df), n = @byrow :g == 1 ? 100 : 500).n == [100, 100, 100, 500, 500]

    @test @select!(copy(df), n = 1).n == fill(1, nrow(df))

    @test @select!(copy(df), cols("new_column") = :i).new_column == df.i
    @test @select!(copy(df), cols(n_str) = :i).new_column == df.i
    @test @select!(copy(df), cols(n_str) = cols(:i) .+ 0).new_column == df.i
    @test @select!(copy(df), cols(n_sym) = :i).new_column == df.i
    @test @select!(copy(df), cols(n_space) = :i)."new column" == df.i
    @test @select!(copy(df), cols("new" * "_" * "column") = :i).new_column == df.i

    # non-copying
    newcol = [1:5;]
    df2 = copy(df)
    df2.newcol = newcol
    @test @select!(df2, :newcol).newcol === newcol

    # mutating
    df2 = @select(df, :i)
    @test @select!(df, :i) === df
    @test df == df2
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
    @test_throws ArgumentError @eval @select(df, y = :i + cols(1))
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
    @test  @with(df, cols(:A) .+ cols("B"))  ==  df.A .+ df.B

    @test_throws ArgumentError @with(df, :A + cols(2))

    @test  x == sum(df.A .* df.B)
    @test  @with(df, df[:A .> 1, ^([:B, :A])]) == df[df.A .> 1, [:B, :A]]
    @test  @with(df, DataFrame(a = :A * 2, b = :A .+ :B)) == DataFrame(a = df.A * 2, b = df.A .+ df.B)
end

@testset "where" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    x = [2, 1, 0, 0]

    @test @where(df, :A .> 1) == df[(df.A .> 1) .=== true,:]
    @test @where(df, :B .> 1) == df[df.B .> 1,:]
    @test @where(df, :A .> x) == df[(df.A .> x) .=== true,:]
    @test @where(df, :B .> x) ≅ df[df.B .> x,:]
    @test @where(df, :A .> :B, :B .> mean(:B)) == DataFrame(A = 3, B = 2)
    @test @where(df, :A .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
    @test @where(df, :A .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]

    @test @where(df, @byrow :A > 1) == df[(df.A .> 1) .=== true,:]
    @test @where(df, @byrow :B > 1) == df[df.B .> 1,:]

    @test @where(df, :A .> 1).A isa Vector{Union{Missing, Int}}

    @test @where(df, cols(:A) .> 1) == df[(df.A .> 1) .=== true,:]
    @test @where(df, cols(:B) .> 1) == df[df.B .> 1,:]
    @test @where(df, cols(:A) .> x) == df[(df.A .> x) .=== true,:]
    @test @where(df, cols(:B) .> x) ≅ df[df.B .> x,:]
    @test @where(df, cols(:A) .> :B, cols(:B) .> mean(:B)) == DataFrame(A = 3, B = 2)
    @test @where(df, cols(:A) .> 1, :B .> 1) == df[map(&, df.A .> 1, df.B .> 1),:]
    @test @where(df, cols(:A) .> 1, :A .< 4, :B .> 1) == df[map(&, df.A .> 1, df.A .< 4, df.B .> 1),:]

    @test @where(df, :A .> 1, :A .<= 2) == DataFrame(A = 2, B = 1)

    subdf = @view df[df.B .== 2, :]

    @test @where(subdf, :A .== 3) == DataFrame(A = 3, B = 2)
end

@testset "orderby" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    @test @orderby(df, :c) ≅ df[[3, 1, 2, 4, 5], :]
    @test @orderby(df, -:g) ≅ df[[4, 5, 1, 2, 3], :]
    @test @orderby(df, :t) ≅ df[[1, 2, 3, 4, 5], :]

    @test @orderby(df, identity(:g), :g.^2) ≅ df[[1, 2, 3, 4, 5], :]

    @test @orderby(df, @byrow :g, @byrow :g^2) ≅ df[[1, 2, 3, 4, 5], :]

    subdf = @view df[1:3, :]

    @test @orderby(subdf, -:i) == df[[3, 2, 1], :]
end

@testset "cols with @select fix" begin
    df = DataFrame("X" => 1, "X Y Z" => 2)

    @test @select(df, cols("X")) == select(df, "X")
    @test @select(df, cols("X Y Z")) == select(df, "X Y Z")
    @test @transform(df, cols("X")) == df
    @test @transform(df, cols("X Y Z")) == df
end


end # module
