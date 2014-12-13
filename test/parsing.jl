using Base.Test
using DataFramesMeta

# for some reason, when a macro takes an expression, symbols like :a
# becomes Expr( :quote, :a ).
# whereas the same expression using :( ) # becomes QuoteNode( :a ).
# This function bridges the difference so
# that we can test the macro's inner components separately.
function convertExpression!( ex::Expr )
    for i in 1:length( ex.args )
        a = ex.args[i]
        if typeof( a ) == QuoteNode
            ex.args[i] = Expr( :quote, a.value )
        elseif typeof( a ) == Expr
            convertExpression!( a )
        end
    end
end

# testing basic functionalities
membernames = Dict{Symbol,Symbol}()
ex = :( mean( :X ) )
convertExpression!( ex )
new_ex = DataFramesMeta.replace_syms( ex, membernames )

@test new_ex.head == :call
@test new_ex.args[1] == :mean
@test new_ex.args[2] != ex.args[2]
@test collect( keys( membernames ) ) == [ :X ]

# avoid parsing :X => 1

membernames = Dict{Symbol,Symbol}()
ex = :( myfunc( :X, hints = Dict{Symbol,Any}( :fancy => true ) ) )
convertExpression!( ex )
new_ex = DataFramesMeta.replace_syms( ex, membernames )
@test collect( keys( membernames ) ) == [ :X ]
