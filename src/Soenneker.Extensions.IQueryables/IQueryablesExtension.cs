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
using System.Globalization;
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
/// <summary>
/// Represents the i queryables extension.
/// </summary>
public static class IQueryablesExtension
{
    /// <summary> (root-type, full path) → property chain </summary>
    private static readonly ConcurrentDictionary<(Type, string), PropertyInfo[]> _propertyChainCache = new();

    /// <summary> Type → (segment → PropertyInfo), case-insensitive, includes CLR names and [JsonPropertyName]. </summary>
    private static readonly ConcurrentDictionary<Type, Dictionary<string, PropertyInfo>> _propertyMapCache = new();

    private static readonly MethodInfo _miOrderBy = GetQueryableMethod(nameof(Queryable.OrderBy));
    private static readonly MethodInfo _miOrderByDesc = GetQueryableMethod(nameof(Queryable.OrderByDescending));
    private static readonly MethodInfo _miThenBy = GetQueryableMethod(nameof(Queryable.ThenBy));
    private static readonly MethodInfo _miThenByDesc = GetQueryableMethod(nameof(Queryable.ThenByDescending));

    private static readonly MethodInfo _stringContains =
        typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string) })!;

    /// <summary>
    /// Executes the where dynamic equals operation.
    /// </summary>
    /// <typeparam name="T">The T type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="field">The field.</param>
    /// <param name="value">The value.</param>
    /// <returns>The result of the operation.</returns>
    [Pure]
    public static IQueryable<T> WhereDynamicEquals<T>(this IQueryable<T> source, string field, object? value)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        Expression body = BuildEqualsBody<T>(param, field, value);
        return source.Where(Expression.Lambda<Func<T, bool>>(body, param));
    }

    /// <summary>
    /// Executes the where dynamic range operation.
    /// </summary>
    /// <typeparam name="T">The T type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="range">The range.</param>
    /// <returns>The result of the operation.</returns>
    [Pure]
    public static IQueryable<T> WhereDynamicRange<T>(this IQueryable<T> source, RangeFilter range)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        Expression? body = BuildRangeBody<T>(param, range);
        return body is null ? source : source.Where(Expression.Lambda<Func<T, bool>>(body, param));
    }

    /// <summary>
    /// Executes the where dynamic search operation.
    /// </summary>
    /// <typeparam name="T">The T type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="search">The search.</param>
    /// <param name="fields">The fields.</param>
    /// <returns>The result of the operation.</returns>
    [Pure]
    public static IQueryable<T> WhereDynamicSearch<T>(this IQueryable<T> source, string search, List<string> fields)
    {
        if (search.IsNullOrWhiteSpace() || fields.Count == 0)
            return source;

        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        Expression? body = BuildSearchBody<T>(param, search, fields);
        return body is null ? source : source.Where(Expression.Lambda<Func<T, bool>>(body, param));
    }

    /// <summary>
    /// Executes the order by dynamic operation.
    /// </summary>
    /// <typeparam name="T">The T type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="field">The field.</param>
    /// <param name="descending">The descending.</param>
    /// <returns>The result of the operation.</returns>
    [Pure]
    public static IOrderedQueryable<T> OrderByDynamic<T>(this IQueryable<T> source, string field, bool descending)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, field);
        LambdaExpression lambda = Expression.Lambda(member, param);

        MethodInfo mi = (descending ? _miOrderByDesc : _miOrderBy).MakeGenericMethod(typeof(T), member.Type);
        MethodCallExpression call = Expression.Call(mi, source.Expression, Expression.Quote(lambda));
        return (IOrderedQueryable<T>)source.Provider.CreateQuery(call);
    }

    /// <summary>
    /// Executes the then by dynamic operation.
    /// </summary>
    /// <typeparam name="T">The T type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="field">The field.</param>
    /// <param name="descending">The descending.</param>
    /// <returns>The result of the operation.</returns>
    [Pure]
    public static IOrderedQueryable<T> ThenByDynamic<T>(this IOrderedQueryable<T> source, string field, bool descending)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, field);
        LambdaExpression lambda = Expression.Lambda(member, param);

        MethodInfo mi = (descending ? _miThenByDesc : _miThenBy).MakeGenericMethod(typeof(T), member.Type);
        MethodCallExpression call = Expression.Call(mi, source.Expression, Expression.Quote(lambda));
        return (IOrderedQueryable<T>)source.Provider.CreateQuery(call);
    }

    /// <summary> Applies filters, search, ordering, paging in one go. </summary>
    [Pure]
    public static IQueryable<T> AddRequestDataOptions<T>(this IQueryable<T> query, RequestDataOptions opts)
    {
        // BIG WIN: build a single predicate and apply one Where() instead of N Where() calls.
        Expression? combined = null;
        ParameterExpression param = Expression.Parameter(typeof(T), "x");

        if (opts.Filters is { Count: > 0 })
        {
            foreach (ExactMatchFilter f in opts.Filters)
            {
                if (f.Field.IsNullOrWhiteSpace())
                    continue;

                Expression eq = BuildEqualsBody<T>(param, f.Field, f.Value);
                combined = combined is null ? eq : Expression.AndAlso(combined, eq);
            }
        }

        if (opts.RangeFilters is { Count: > 0 })
        {
            foreach (RangeFilter r in opts.RangeFilters)
            {
                if (r.Field.IsNullOrWhiteSpace())
                    continue;

                Expression? range = BuildRangeBody<T>(param, r);
                if (range is null)
                    continue;

                combined = combined is null ? range : Expression.AndAlso(combined, range);
            }
        }

        if (opts.Search.HasContent() && opts.SearchFields is { Count: > 0 })
        {
            Expression? search = BuildSearchBody<T>(param, opts.Search, opts.SearchFields);
            if (search is not null)
                combined = combined is null ? search : Expression.AndAlso(combined, search);
        }

        if (combined is not null)
            query = query.Where(Expression.Lambda<Func<T, bool>>(combined, param));

        if (opts.OrderBy is { Count: > 0 })
        {
            bool first = true;

            foreach (OrderByOption o in opts.OrderBy)
            {
                bool desc = o.Direction == SortDirection.Desc;
                query = first
                    ? query.OrderByDynamic(o.Field, desc)
                    : ((IOrderedQueryable<T>)query).ThenByDynamic(o.Field, desc);

                first = false;
            }
        }

        return query;
    }

    private static Expression BuildEqualsBody<T>(ParameterExpression param, string field, object? value)
    {
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
            object? coerced = ChangeTypeIfNeeded(value, constType);

            rhs = Expression.Constant(coerced, constType);
            if (constType != member.Type)
                rhs = Expression.Convert(rhs, member.Type);
        }

        return Expression.Equal(member, rhs);
    }

    private static Expression? BuildRangeBody<T>(ParameterExpression param, RangeFilter range)
    {
        MemberExpression member = BuildMemberAccess<T>(param, range.Field);
        Expression? body = null;

        Add(range.GreaterThan, Expression.GreaterThan);
        Add(range.GreaterThanOrEqual, Expression.GreaterThanOrEqual);
        Add(range.LessThan, Expression.LessThan);
        Add(range.LessThanOrEqual, Expression.LessThanOrEqual);

        return body;

        void Add(object? v, Func<Expression, Expression, BinaryExpression> op)
        {
            if (v is null) return;

            Type targetNonNull = GetNonNullableType(member.Type);
            object? coerced = ChangeTypeIfNeeded(v, targetNonNull);

            ConstantExpression constant = Expression.Constant(coerced, targetNonNull);
            Expression rhs = targetNonNull == member.Type ? constant : Expression.Convert(constant, member.Type);

            Expression next = op(member, rhs);
            body = body is null ? next : Expression.AndAlso(body, next);
        }
    }

    private static Expression? BuildSearchBody<T>(ParameterExpression param, string search, List<string> fields)
    {
        if (search.IsNullOrWhiteSpace() || fields.Count == 0)
            return null;

        // Reuse constant (avoid allocating ConstantExpression per field)
        ConstantExpression searchConst = Expression.Constant(search);

        Expression? body = null;

        for (int i = 0; i < fields.Count; i++)
        {
            string field = fields[i];
            if (field.IsNullOrWhiteSpace())
                continue;

            MemberExpression member = BuildMemberAccess<T>(param, field);
            if (member.Type != typeof(string))
                continue;

            MethodCallExpression call = Expression.Call(member, _stringContains, searchConst);
            body = body is null ? call : Expression.OrElse(body, call);
        }

        return body;
    }

    /// <summary> Resolve property chain for path like "A.B.C". Uses per-type property map cache. </summary>
    private static MemberExpression BuildMemberAccess<T>(ParameterExpression root, string path)
    {
        ValidateFieldPath(path);

        PropertyInfo[] chain = _propertyChainCache.GetOrAdd((typeof(T), path), static key =>
        {
            (Type current, string remaining) = key;

            // Avoid LINQ; small list size expected
            var props = new List<PropertyInfo>(4);

            while (true)
            {
                (string seg, string? tail) = SplitFirst(remaining);

                PropertyInfo match = FindSegmentProperty(current, seg)
                                     ?? throw new ArgumentException($"Field \"{seg}\" does not exist on type {current.Name}");

                props.Add(match);
                current = match.PropertyType;

                if (tail is null)
                    break;

                remaining = tail;
            }

            int count = props.Count;
            var result = new PropertyInfo[count];
            for (int i = 0; i < count; i++)
                result[i] = props[i];

            return result;
        });

        Expression expr = root;

        for (int i = 0; i < chain.Length; i++)
            expr = Expression.Property(expr, chain[i]);

        return (MemberExpression)expr;
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

        for (int i = 0; i < fieldPath.Length; i++)
        {
            char c = fieldPath[i];
            bool ok = (c is >= 'A' and <= 'Z')
                      || (c is >= 'a' and <= 'z')
                      || (c is >= '0' and <= '9')
                      || c == '_'
                      || c == '.';

            if (!ok)
                throw new ArgumentException($"Invalid field name: \"{fieldPath}\"");
        }
    }

    private static Type GetNonNullableType(Type t) => Nullable.GetUnderlyingType(t) ?? t;

    private static MethodInfo GetQueryableMethod(string name)
    {
        // Find Queryable.{name}<TSource,TKey>(IQueryable<TSource>, Expression<Func<TSource,TKey>>)
        MethodInfo[] methods = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static);

        for (int i = 0; i < methods.Length; i++)
        {
            MethodInfo m = methods[i];
            if (!string.Equals(m.Name, name, StringComparison.Ordinal))
                continue;

            ParameterInfo[] ps = m.GetParameters();
            if (ps.Length != 2)
                continue;

            if (!m.IsGenericMethodDefinition)
                continue;

            // good enough discriminator for these four methods
            return m;
        }

        throw new InvalidOperationException($"Could not locate Queryable.{name} method.");
    }

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

        if (target.IsEnum)
            return value is string s ? Enum.Parse(target, s, ignoreCase: true) : Enum.ToObject(target, value);

        if (target == typeof(Guid))
        {
            if (value is Guid g) return g;
            if (value is string sg) return Guid.Parse(sg);
            if (value is byte[] bytes) return new Guid(bytes);
        }

        if (target == typeof(DateTimeOffset))
        {
            if (value is DateTimeOffset dto) return dto;
            if (value is string sdt) return DateTimeOffset.Parse(sdt, CultureInfo.InvariantCulture);
        }

        if (target == typeof(DateTime))
        {
            if (value is DateTime dt) return dt;
            if (value is string sdt2) return DateTime.Parse(sdt2, CultureInfo.InvariantCulture);
        }

        // Fast path for common primitives when coming from string
        if (value is string str)
        {
            switch (Type.GetTypeCode(target))
            {
                case TypeCode.Boolean: return bool.Parse(str);
                case TypeCode.Int16: return short.Parse(str, NumberStyles.Integer, CultureInfo.InvariantCulture);
                case TypeCode.Int32: return int.Parse(str, NumberStyles.Integer, CultureInfo.InvariantCulture);
                case TypeCode.Int64: return long.Parse(str, NumberStyles.Integer, CultureInfo.InvariantCulture);
                case TypeCode.UInt16: return ushort.Parse(str, NumberStyles.Integer, CultureInfo.InvariantCulture);
                case TypeCode.UInt32: return uint.Parse(str, NumberStyles.Integer, CultureInfo.InvariantCulture);
                case TypeCode.UInt64: return ulong.Parse(str, NumberStyles.Integer, CultureInfo.InvariantCulture);
                case TypeCode.Single: return float.Parse(str, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
                case TypeCode.Double: return double.Parse(str, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
                case TypeCode.Decimal: return decimal.Parse(str, NumberStyles.Number, CultureInfo.InvariantCulture);
                case TypeCode.Byte: return byte.Parse(str, NumberStyles.Integer, CultureInfo.InvariantCulture);
                case TypeCode.SByte: return sbyte.Parse(str, NumberStyles.Integer, CultureInfo.InvariantCulture);
                case TypeCode.Char: return str.Length > 0 ? str[0] : throw new FormatException("Empty string cannot be converted to char.");
            }
        }

        return Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
    }
}
