module TestRename

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "rename" begin
   
    df = DataFrame(old_col1 = rand(10), old_col2 = rand([missing;2:10],10),old_col3 = rand(10));

    # rename test set
    @test @rename(df, :new1 = :old_col1) ≅ rename(df, :old_col1 => :new1)
    @test @rename(df, :new1 = $"old_col1", :new2 = $"old_col2") ≅ rename(df, [:old_col1, :old_col2] .=> [:new1,:new2] )
    @test @rename(df, :new1 = :old_col1, :new2 = :old_col2) ≅ rename(df, [:old_col1, :old_col2] .=> [:new1,:new2] )
    @test @rename(df, :new1 = :old_col1, :new2 = $"old_col2") ≅ rename(df, [:old_col1, :old_col2] .=> [:new1,:new2] )
    @test @rename(df, :new1 = $("old_col" *"1"), :new2 = :old_col2) ≅ rename(df, [:old_col1, :old_col2] .=> [:new1,:new2] )

    @test @rename(df, $("old_col1" => "new1") ) ≅ rename(df, :old_col1 .=> :new1 )
    @test @rename(df, $(:old_col1 => :new1)) ≅ rename(df, :old_col1 .=> :new1 )
    @test @rename(df, $(:old_col1 => "new1"), $(:old_col2 => :new2) ) ≅ rename(df, [:old_col1, :old_col2] .=> [:new1,:new2] )
    @test @rename(df, $(:old_col1 => "new1"), :new2 = :old_col2) ≅ rename(df, [:old_col1, :old_col2] .=> [:new1,:new2] )
    
    res = @rename df :new1 = begin
        $("old_col" * "1")
    end
    
    @test res ≅ rename(df, :old_col1 .=> :new1 )
    
end


@testset "rename with :block" begin
    df = DataFrame(old_col1 = rand(10), old_col2 = rand(10),old_col3 = rand(10));
    
    res = @rename df begin
        :new1 = :old_col1
        :new2 = :old_col2
        :new3 = $"old_col3"    
    end

   @test res ≅ @rename(df, :new1 =:old_col1, :new2 = :old_col2, :new3 = :old_col3)  

    res = @rename copy(df) begin
        :new1 = $"old_col1"
        :new2 = $"old_col2"
    end
    
    @test res ≅ @rename(df, :new1 =:old_col1, :new2 = :old_col2)  


    res = @rename df begin
        :new1 = $("old_col" * "1")        
    end
    
    @test res ≅ @rename(df, :new1 =:old_col1)  

    res = @rename df begin
       $("old_col" * "1" => String(:new1))        
    end
    
    @test res ≅ @rename(df, :new1 = :old_col1)  


    subdf = @view df[df.old_col1 .< .5, :]
    res = @rename copy(subdf) begin
        :new1 = :old_col1
    end

    @test res ≅ @rename(subdf, :new1 = :old_col1)

end


@testset "rename!" begin   
    df = DataFrame(old_col1 = rand(10), old_col2 = rand(10),old_col3 = rand(10));

    # rename test set
    @test @rename!(copy(df), :new1 = :old_col1) ≅ rename(copy(df), :old_col1 => :new1)
    @test @rename!(copy(df), :new1 = $"old_col1", :new2 = $"old_col2") ≅ rename(copy(df), [:old_col1, :old_col2] .=> [:new1,:new2] )
    @test @rename!(copy(df), :new1 = :old_col1, :new2 = :old_col2) ≅ rename(copy(df), [:old_col1, :old_col2] .=> [:new1,:new2] )
    @test @rename!(copy(df), :new1 = :old_col1, :new2 = $"old_col2") ≅ rename(copy(df), [:old_col1, :old_col2] .=> [:new1,:new2] )
    @test @rename!(copy(df), :new1 = $("old_col" *"1"), :new2 = :old_col2) ≅ rename(copy(df), [:old_col1, :old_col2] .=> [:new1,:new2] )

    @test @rename!(copy(df), $("old_col1" => "new1") ) ≅ rename(copy(df), :old_col1 .=> :new1 )
    @test @rename!(copy(df), $(:old_col1 => :new1)) ≅ rename(copy(df), :old_col1 .=> :new1 )
    @test @rename!(copy(df), $(:old_col1 => "new1"), $(:old_col2 => :new2)) ≅ rename(copy(df), [:old_col1,:old_col2] .=> [:new1,:new2] )
    @test @rename!(copy(df), $(:old_col1 => "new1"), :new2 = :old_col2) ≅ rename(copy(df), [:old_col1, :old_col2] .=> [:new1,:new2] )

    res = @rename! copy(df) :new1 = begin
        $("old_col" * "1")
    end
    @test res ≅ rename!(copy(df), :old_col1 .=> :new1 )        

end



@testset "rename! with :block" begin
    df = DataFrame(old_col1 = rand(10), old_col2 = rand(10), old_col3 = rand(10));
    
    res = @rename! copy(df) begin
        :new1 = :old_col1
        :new2 = :old_col2
        :new3 = $"old_col3"    
    end

   @test res ≅ @rename!(copy(df), :new1 =:old_col1, :new2 = :old_col2, :new3 = :old_col3)  

    res = @rename! copy(df) begin
        :new1 = $"old_col1"
        :new2 = $"old_col2"
    end
    
    @test res ≅ @rename!(copy(df), :new1 =:old_col1, :new2 = :old_col2)  

    res = @rename! copy(df) begin
        :new1 = $("old_col" * "1")        
    end
    
    @test res ≅ @rename!(copy(df), :new1 =:old_col1)  

    res = @rename! copy(df) begin
       $("old_col" * "1" => string(:new1))        
    end
    
    @test res ≅ @rename!(copy(df), :new1 = :old_col1)  

    subdf = @view df[df.old_col1 .< .5, :]
    d = @rename! copy(subdf) begin
        :new1 = :old_col1
    end

    @test d ≅ @rename!(copy(subdf), :new1 = :old_col1)

end

end # module