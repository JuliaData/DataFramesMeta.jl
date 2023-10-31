module TestImport

using Test
import DataFrames
import DataFramesMeta

@testset "importing error" begin
    df = DataFrames.DataFrame(a = [1, 2, 3])
    t = DataFramesMeta.@rsubset df :a > 1
    @test t == DataFrames.DataFrame(a = [2, 3])
    t = DataFramesMeta.@transform df begin
        @byrow :z = :a == 2
    end
    @test t == DataFrames.DataFrame(a = [1, 2, 3], z = [false, true, false])

    t = DataFramesMeta.@rtransform df @astable begin
        :b = 1
        :c = 2
    end
    @test t == DataFrames.DataFrame(a = [1, 2, 3], b = [1, 1, 1], c  = [2, 2, 2])

    # AsTable on the RHS relies on the literal "AsTable" appearing
    # so we test that this throws an error
    @test_throws UndefVarError t = DataFramesMeta.@transform df :c = sum(AsTable([:a]))

    # And confusingly, if you use DataFrames.AsTable on the RHS, none of the
    # special escaping happens.
    # There is not a lot to do about this, unfortunately. I don't want
    # to modify user-code.
    @test_throws ArgumentError DataFramesMeta.@transform df :c = sum(DataFrames.AsTable([:a]))
end


end # module