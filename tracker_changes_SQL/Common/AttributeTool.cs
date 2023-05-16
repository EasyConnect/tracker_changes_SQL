namespace tracker_changes_SQL.Common
{
    public static class AttributeTool
    {
        public static T? GetAttribute<T>(Type type) where T : Attribute
        {
            return (T?)Attribute.GetCustomAttribute(type, typeof(T)) ?? null;
        }
    }
}
}
