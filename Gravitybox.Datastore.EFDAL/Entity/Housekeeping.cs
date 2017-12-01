namespace Gravitybox.Datastore.EFDAL.Entity
{
    public enum HousekeepingTaskType
    {
        ClearRepositoryLog = 1,
    }

    partial class Housekeeping
    {
        public HousekeepingTaskType TypeValue { get { return (HousekeepingTaskType)this.Type; } }
    }
}
