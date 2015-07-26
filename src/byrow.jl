
export @byrow

##############################################################################
##
## @byrow
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

function byrow_helper(df, body)
    e_delimiters = Expr(:(=), :row, :( 1:length($df[1]) ))
    e_forloop = Expr(:for, e_delimiters, byrow_replace(body))
    return Expr(:macrocall, :(DataFramesMeta.@with), df, e_forloop)
end

macro byrow(df, body)
    esc(byrow_helper(df, body))
    df
end
