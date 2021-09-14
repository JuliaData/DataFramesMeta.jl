function conditionally_add_symbols!(inputs_to_function, lhs_assignments, col)
    # if it's already been assigned at top-level,
    # don't add it to the inputs
    if haskey(lhs_assignments, col)
        return lhs_assignments[col]
    else
        return addkey!(inputs_to_function, col)
    end
end

replace_syms_astable!(inputs_to_function, lhs_assignments, x) = x
replace_syms_astable!(inputs_to_function, lhs_assignments, q::QuoteNode) =
    conditionally_add_symbols!(inputs_to_function, lhs_assignments, q)

function replace_syms_astable!(inputs_to_function, lhs_assignments, e::Expr)
    if onearg(e, :^)
        return e.args[2]
    end

    col = get_column_expr(e)
    if col !== nothing
        return conditionally_add_symbols!(inputs_to_function, lhs_assignments, col)
    elseif e.head == :.
        return replace_dotted_astable!(inputs_to_function, lhs_assignments, e)
    else
        return mapexpr(x -> replace_syms_astable!(inputs_to_function, lhs_assignments, x), e)
    end
end

protect_replace_syms_astable!(inputs_to_function, lhs_assignments, e) = e
protect_replace_syms_astable!(inputs_to_function, lhs_assignments, e) =
    replace_syms!(inputs_to_function, lhs_assignments, e)

function replace_dotted_astable!(inputs_to_function, lhs_assignments, e)
    x_new = replace_syms_astable!(inputs_to_function, lhs_assignments, e.args[1])
    y_new = protect_replace_syms_astable!(inputs_to_function, lhs_assignments, e.args[2])
    Expr(:., x_new, y_new)
end

is_column_assigment(ex) = false
function is_column_assigment(ex::Expr)
    ex.head == :(=) && (get_column_expr(ex.args[1]) !== nothing)
end

function collect_top_level_column_assignments(ex)
    inputs_to_function = Dict{Any, Symbol}()
    lhs_assignments = Dict{Any, Symbol}()

    ex = MacroTools.flatten(ex)
    exprs = map(ex.args) do arg
        @show arg
        @show is_column_assigment(arg)
        if is_column_assigment(arg)
            lhs = arg.args[1]
            rhs = arg.args[2]
            new_ex = replace_syms_astable!(inputs_to_function, lhs_assignments, arg.args[2])
            if haskey(inputs_to_function, lhs)
                new_lhs = inputs_to_function[lhs]
            else
                new_lhs = addkey!(lhs_assignments, lhs)
            end

            Expr(:(=), new_lhs, new_ex)
        else
            replace_syms_astable!(inputs_to_function, lhs_assignments, arg)
        end
    end
    cols_to_add = collect(keys(inputs_to_function))
    new_ex = Expr(:block, exprs...)
end