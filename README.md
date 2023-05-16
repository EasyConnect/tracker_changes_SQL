# tracker_changes_SQL

The project by create trigers in the database with logger the changes in the tables in database

The table with the register changes is 'TrackLog'

The entities need two attributes: Track and Table

## Usage
```c#
//Class
namespace Project.Model.Entities
{
    [Track]
    [Table("person", Schema = "dbo")]
    public class Person
    {
    }
}

//Startup
var trackLogger = new TrackerTriggerSQLServer("DBContext");
trackLogger.AddTriggers("Project.Model.Entities");
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
