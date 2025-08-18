using Soenneker.Dtos.Filters.ExactMatch;
using Soenneker.Dtos.Filters.Range;
using Soenneker.Dtos.Options.OrderBy;
using Soenneker.Dtos.RequestDataOptions;
using Soenneker.Enums.SortDirections;
using Soenneker.Extensions.String;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization;

namespace Soenneker.Extensions.IQueryables;

/// <summary>
/// A collection of helpful IQueryable extension methods
/// </summary>
// ReSharper disable once UnusedType.Global
// ReSharper disable once InconsistentNaming
public static class IQueryablesExtension
{
    /// <summary> (root-type, full path) → property chain </summary>
    private static readonly ConcurrentDictionary<(Type, string), PropertyInfo[]> _propertyChainCache = new();

    /// <summary> Type → (segment → PropertyInfo), case-insensitive, includes CLR names and [JsonPropertyName]. </summary>
    private static readonly ConcurrentDictionary<Type, Dictionary<string, PropertyInfo>> _propertyMapCache = new();

    private static readonly MethodInfo _miOrderBy =
        typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.OrderBy) && m.GetParameters().Length == 2);

    private static readonly MethodInfo _miOrderByDesc =
        typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.OrderByDescending) && m.GetParameters().Length == 2);

    private static readonly MethodInfo _miThenBy =
        typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.ThenBy) && m.GetParameters().Length == 2);

    private static readonly MethodInfo _miThenByDesc =
        typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.ThenByDescending) && m.GetParameters().Length == 2);

    private static readonly MethodInfo _stringContains = typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!;

    [Pure]
    public static IQueryable<T> WhereDynamicEquals<T>(this IQueryable<T> source, string field, object? value)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, field);

        Expression rhs;
        if (value is null)
        {
            // Ensure constant carries the member type for provider translation
            rhs = Expression.Constant(null, member.Type);
        }
        else
        {
            Type constType = GetNonNullableType(member.Type);
            // If incoming value already matches, use typed constant; else rely on convert
            rhs = Expression.Constant(ChangeTypeIfNeeded(value, constType), constType);
            if (constType != member.Type)
                rhs = Expression.Convert(rhs, member.Type);
        }

        BinaryExpression body = Expression.Equal(member, rhs);
        return source.Where(Expression.Lambda<Func<T, bool>>(body, param));
    }

    [Pure]
    public static IQueryable<T> WhereDynamicRange<T>(this IQueryable<T> source, RangeFilter range)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, range.Field);
        Expression? body = null;

        Add(range.GreaterThan, Expression.GreaterThan);
        Add(range.GreaterThanOrEqual, Expression.GreaterThanOrEqual);
        Add(range.LessThan, Expression.LessThan);
        Add(range.LessThanOrEqual, Expression.LessThanOrEqual);

        return body is null ? source : source.Where(Expression.Lambda<Func<T, bool>>(body, param));

        void Add(object? v, Func<Expression, Expression, BinaryExpression> op)
        {
            if (v is null) return;

            Type targetNonNull = GetNonNullableType(member.Type);
            ConstantExpression constant = Expression.Constant(ChangeTypeIfNeeded(v, targetNonNull), targetNonNull);
            Expression rhs = targetNonNull == member.Type ? constant : Expression.Convert(constant, member.Type);

            body = body is null ? op(member, rhs) : Expression.AndAlso(body, op(member, rhs));
        }
    }

    [Pure]
    public static IQueryable<T> WhereDynamicSearch<T>(this IQueryable<T> source, string search, List<string> fields)
    {
        if (search.IsNullOrWhiteSpace() || fields.Count == 0)
            return source;

        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        Expression? body = null;

        foreach (string field in fields)
        {
            MemberExpression member = BuildMemberAccess<T>(param, field);
            if (member.Type != typeof(string))
                continue;

            MethodCallExpression call = Expression.Call(member, _stringContains, Expression.Constant(search));
            body = body is null ? call : Expression.OrElse(body, call);
        }

        return body is null ? source : source.Where(Expression.Lambda<Func<T, bool>>(body, param));
    }

    [Pure]
    public static IOrderedQueryable<T> OrderByDynamic<T>(this IQueryable<T> source, string field, bool descending)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, field);
        LambdaExpression lambda = Expression.Lambda(member, param);

        MethodInfo mi = (descending ? _miOrderByDesc : _miOrderBy).MakeGenericMethod(typeof(T), member.Type);
        // Build the expression rather than MethodInfo.Invoke to avoid reflection invocation costs.
        MethodCallExpression call = Expression.Call(mi, source.Expression, Expression.Quote(lambda));
        return (IOrderedQueryable<T>) source.Provider.CreateQuery(call);
    }

    [Pure]
    public static IOrderedQueryable<T> ThenByDynamic<T>(this IOrderedQueryable<T> source, string field, bool descending)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, field);
        LambdaExpression lambda = Expression.Lambda(member, param);

        MethodInfo mi = (descending ? _miThenByDesc : _miThenBy).MakeGenericMethod(typeof(T), member.Type);
        MethodCallExpression call = Expression.Call(mi, source.Expression, Expression.Quote(lambda));
        return (IOrderedQueryable<T>) source.Provider.CreateQuery(call);
    }

    /// <summary> Applies filters, search, ordering, paging in one go. </summary>
    [Pure]
    public static IQueryable<T> AddRequestDataOptions<T>(this IQueryable<T> query, RequestDataOptions opts)
    {
        if (opts.Filters is {Count: > 0})
        {
            foreach (ExactMatchFilter f in opts.Filters)
            {
                query = query.WhereDynamicEquals(f.Field, f.Value);
            }
        }

        if (opts.RangeFilters is {Count: > 0})
        {
            foreach (RangeFilter r in opts.RangeFilters)
            {
                query = query.WhereDynamicRange(r);
            }
        }

        if (opts.Search.HasContent() && opts.SearchFields is {Count: > 0})
            query = query.WhereDynamicSearch(opts.Search, opts.SearchFields);

        if (opts.OrderBy is {Count: > 0})
        {
            var first = true;

            foreach (OrderByOption o in opts.OrderBy)
            {
                bool desc = o.Direction == SortDirection.Desc;
                query = first ? query.OrderByDynamic(o.Field, desc) : ((IOrderedQueryable<T>) query).ThenByDynamic(o.Field, desc);
                first = false;
            }
        }

        return query;
    }

    /// <summary> Resolve property chain for path like "A.B.C". Uses per-type property map cache. </summary>
    private static MemberExpression BuildMemberAccess<T>(ParameterExpression root, string path)
    {
        ValidateFieldPath(path);

        PropertyInfo[] chain = _propertyChainCache.GetOrAdd((typeof(T), path), static key =>
        {
            (Type current, string remaining) = key;
            var props = new List<PropertyInfo>(4); // small default; grows if needed

            while (true)
            {
                (string seg, string? tail) = SplitFirst(remaining);
                PropertyInfo match = FindSegmentProperty(current, seg) ?? throw new ArgumentException($"Field \"{seg}\" does not exist on type {current.Name}");

                props.Add(match);
                current = match.PropertyType;

                if (tail is null) break;
                remaining = tail;
            }

            return props.ToArray();
        });

        Expression expr = root;

        foreach (PropertyInfo p in chain)
        {
            expr = Expression.Property(expr, p);
        }

        return (MemberExpression) expr;
    }

    /// <summary> Resolve ONE path segment on <paramref name="type"/> using the cached property map. </summary>
    private static PropertyInfo? FindSegmentProperty(Type type, string seg)
    {
        Dictionary<string, PropertyInfo> map = _propertyMapCache.GetOrAdd(type, static t =>
        {
            // Build once per type. Include both CLR names and [JsonPropertyName].
            var dict = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);

            foreach (PropertyInfo p in t.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                dict[p.Name] = p;
                string? json = p.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name;

                if (json.HasContent())
                    dict[json!] = p;
            }

            return dict;
        });

        map.TryGetValue(seg, out PropertyInfo? pi);
        return pi;
    }

    /// <summary> Splits once on '.', returns (head, tailOrNull). </summary>
    private static (string head, string? tail) SplitFirst(string dotted)
    {
        int idx = dotted.IndexOf('.');
        return idx < 0 ? (dotted, null) : (dotted[..idx], dotted[(idx + 1)..]);
    }

    /// <summary> Fast validation for [A-Za-z0-9_.]+ (no regex). Throws on invalid. </summary>
    private static void ValidateFieldPath(string fieldPath)
    {
        if (fieldPath.IsNullOrWhiteSpace())
            throw new ArgumentException($"Invalid field name: \"{fieldPath}\"");

        for (var i = 0; i < fieldPath.Length; i++)
        {
            char c = fieldPath[i];
            bool ok = c is >= 'A' and <= 'Z' || c is >= 'a' and <= 'z' || c is >= '0' and <= '9' || c == '_' || c == '.';

            if (!ok)
                throw new ArgumentException($"Invalid field name: \"{fieldPath}\"");
        }
    }

    private static Type GetNonNullableType(Type t) => Nullable.GetUnderlyingType(t) ?? t;

    /// <summary>
    /// Coerces a value to a target type when it differs (e.g., string -> int/DateTime), but
    /// avoids work if already assignable. Keeps nulls as-is.
    /// </summary>
    private static object? ChangeTypeIfNeeded(object? value, Type target)
    {
        if (value is null) 
            return null;

        Type src = value.GetType();

        if (target.IsAssignableFrom(src)) 
            return value;

        // Handle enums and guid explicitly for common cases
        if (target.IsEnum)
            return value is string s ? Enum.Parse(target, s, ignoreCase: true) : Enum.ToObject(target, value);

        if (target == typeof(Guid))
            return value is string sg ? Guid.Parse(sg) : new Guid((byte[]) value);

        if (target == typeof(DateTimeOffset) && value is string sdt)
            return DateTimeOffset.Parse(sdt);

        if (target == typeof(DateTime) && value is string sdt2)
            return DateTime.Parse(sdt2);

        return Convert.ChangeType(value, target);
    }
}