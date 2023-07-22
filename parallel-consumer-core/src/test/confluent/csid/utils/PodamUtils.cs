using Podam.Factories;

namespace Confluent.Csid.Utils
{
    public static class PodamUtils
    {
        public static readonly PodamFactory PODAM_FACTORY = new PodamFactory();

        public static T CreateInstance<T>(Type[] genericTypeArgs)
        {
            return PODAM_FACTORY.ManufacturePojo<T>(genericTypeArgs);
        }
    }
}