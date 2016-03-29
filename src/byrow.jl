
export @byrow!

##############################################################################
##
## @byrow!
##
##############################################################################

# Recursive function that traverses the syntax tree of e, replaces instances of
# ":(:(x))" with ":x[row]".
function byrow_replace(e::Expr)
    # target is the Expr that is built to replace ":(:(x))"
    target = Expr(:ref)

    # Traverse the syntax tree of e
    if e.head != :quote
        return Expr(e.head, (isempty(e.args) ? e.args : map(x -> byrow_replace(x), e.args))...)
    else
        push!(target.args, e, :row)
        return target
    end
end

# Set the base case for helper, i.e. for when expand hits an object of type
# other than Expr (generally a Symbol or a literal).
byrow_replace(x) = x

function byrow_find_newcols(e::Expr, newcol_decl)
    if e.head == :macrocall && e.args[1] == symbol("@newcol")
        ea = e.args[2]
        # expression to assign a new column to df
        return (nothing, Any[Expr(:kw, ea.args[1], Expr(:call, ea.args[2].args[1], ea.args[2].args[2], :_N))])
    else
        if isempty(e.args)
            return (e.args, Any[])
        end
        newargs = Any[]
        for ea in e.args
            (nea, newcol) = byrow_find_newcols(ea, newcol_decl)
            nea != nothing && push!(newargs, nea) 
            nea == nothing && length(newcol) > 0 && append!(newcol_decl, newcol) 
        end
        return (Expr(e.head, newargs...), newcol_decl)
    end
end

byrow_find_newcols(x, newcol_decl) = (x, Any[])

function byrow_helper(df, body)
    (e_body, e_newcols) = byrow_find_newcols(body, Any[])
    e_delimiters = Expr(:(=), :row, :( 1:_N ))
    e_forloop = Expr(:for, e_delimiters, byrow_replace(e_body))
    return quote
        _N = length($df[1])
        _DF = @transform($df, $(e_newcols...))
        @with _DF $e_forloop
        _DF
    end
end

"""
```julia 
@byrow!(d, expr) 
``` 

Act on a DataFrame row-by-row.

Includes support for control flow and `begin end` blocks. Since the
"environment" induced by `@byrow! df` is implicitly a single row of `df`, 
use regular operators and comparisons instead of their elementwise counterparts
as in `@with`. Note that the scope within `@byrow!` is a hard scope.

`byrow!` also supports special syntax for allocating new columns. The syntax
`@newcol x::Array{Int}` allocates a new column `:x` with an `Array` container
with eltype `Int`. Note that the returned `AbstractDataFrame` includes these new
columns, but the original `d` is not affected. This feature makes it easier to
use `byrow!` for data transformations.

### Arguments

* `d` : an `AbstractDataFrame`
* `expr` : expression operated on row by row

### Returns

The modified `AbstractDataFrame`.

### Examples

```julia
df = DataFrame(A = 1:3, B = [2, 1, 2])
let x = 0
    @byrow!(df, if :A + :B == 3; x += 1 end)  #  This doesn't work without the let
    x
end
@byrow! df if :A > :B; :A = 0 end
df2 = @byrow! df begin
    @newcol colX::Array{Float64}
    :colX = :B == 2 ? pi * :A : :B
end
```

"""
macro byrow!(df, body)
    esc(byrow_helper(df, body))
end
