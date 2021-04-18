module Deprecated

using Test
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "@based_on" begin
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

    gd = groupby(df, :g)

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    @test @based_on(gd, n = mean(:i)).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(:i) + mean(:g)).n == [3.0, 6.5]
    @test @based_on(gd, n = first(:t .* string.(:y))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(:y, ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(:y, ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = :i).body == df.i
    @test @based_on(gd, transform = :i).transform == df.i
    @test @based_on(gd, (n1 = [first(:i)], n2 = [first(:y)])).n1 == [1, 4]

    @test @based_on(gd, n = mean(cols(iq))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(iq)) + mean(cols(gq))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tq) .* string.(cols(yq)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yq), ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yq), ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(iq)).body == df.i
    @test @based_on(gd, transform = cols(iq)).transform == df.i
    @test @based_on(gd, (n1 = [first(cols(iq))], n2 = [first(cols(yq))])).n1 == [1, 4]

    @test @based_on(gd, n = mean(cols(ir))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(ir)) + mean(cols(gr))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tr) .* string.(cols(yr)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yr), ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yr), ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(ir)).body == df.i
    @test @based_on(gd, transform = cols(ir)).transform == df.i
    @test @based_on(gd, (n1 = [first(cols(ir))], n2 = [first(cols(yr))])).n1 == [1, 4]
    @test @based_on(gd, n = mean(cols("i")) + 0 * first(cols(:g))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(2)) + first(cols(1))).n == [3.0, 6.5]


    @test @based_on(gd, :i) == select(df, :g, :i)
    @test @based_on(gd, :i, :g) ≅ select(df, :g, :i)

    @test @based_on(gd, :i, n = 1).n == fill(1, nrow(df))

    @test @based_on(gd, cols("new_column") = 2).new_column == [2, 2]
    @test @based_on(gd, cols(n_str) = 2).new_column == [2, 2]
    @test @based_on(gd, cols(n_sym) = 2).new_column == [2, 2]
    @test @based_on(gd, cols(n_space) = 2)."new column" == [2, 2]
    @test @based_on(gd, cols("new" * "_" * "column") = 2)."new_column" == [2, 2]
end

end # module