namespace tracker_changes_SQL.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class TrackAttribute : Attribute
    {
        public TrackAttribute()
        {
        }
    }
}
