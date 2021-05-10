@testset "by with :block" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    g = groupby(df, :g)

    d = @by df :g begin
        im = mean(:i)
        tf = first(:t)
    end
    @test d ≅ @by(df, :g, im = mean(:i), tf = first(:t))

    d = @by df :g begin
        cols(:im) = mean(:i)
        tf = first(:t)
    end
    @test d ≅ @by(df, :g, im = mean(:i), tf = first(:t))

    d = @by df :g begin
        im = mean(:i)
        tf = first(cols(:t))
    end
    @test d ≅ @by(df, :g, im = mean(:i), tf = first(:t))

    d = @by df :g begin
        im = begin
            mean(:i)
        end
        tf = first(:t)
    end
    @test d ≅ @by(df, :g, im = mean(:i), tf = first(:t))
end