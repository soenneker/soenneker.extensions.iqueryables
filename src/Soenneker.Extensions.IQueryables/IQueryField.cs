using System.Linq;
namespace Soenneker.Extensions.IQueryables;
/// <summary>A statically typed field used to order queries by a registered name without runtime generic construction.</summary>
public interface IQueryField<T>
{
    /// <summary>Starts ordering by this field.</summary>
    IOrderedQueryable<T> OrderBy(IQueryable<T> source, bool descending = false);
    /// <summary>Adds this field to an existing ordering.</summary>
    IOrderedQueryable<T> ThenBy(IOrderedQueryable<T> source, bool descending = false);
}
