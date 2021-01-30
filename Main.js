
const QDB = require("qdatabase");
const Enm = require("enmap");
const Jsh = require("@joshdb/core");

const Crypto = require("crypto");
const CLI    = require("cli-color");
const SQL    = require("better-sqlite3");

const FS = require("fs");
if (!FS.existsSync("data/"))
FS.mkdirSync("data/");

const Tables = new Map([
    ["Small", 100],
    ["Medium", 20000],
    ["Large", 50000],
    // ["Enterprise", 800000]
]);

async function Populate (Test, Path, TableCreate, DoTheStuff, GetSize, Close) {
    const Master = new SQL(Path);
    process.stdout.write(CLI.blue(`\n${Test}\n`));

    // Completely remove tables
    Master.prepare("SELECT * FROM 'sqlite_master' WHERE type = 'table';")
    .all().forEach(Entry => Master.prepare(`DROP TABLE '${Entry.name}';`).run());
    Master.close();

    // Create new Connections
    for (const [Table, Size] of Tables) {
        const Connection = await TableCreate(Table);
        process.stdout.write(CLI.white(`· Creating '${CLI.bold(Table)}' table... `));

        const TStart = process.hrtime();

        for (let i = 0; i < Size; i++) {
            await DoTheStuff(Connection, Crypto.randomBytes(8).toString("hex"), {
                Username: [
                    "Jake", "Smally", "Amy", "Foo", "Bar", "Apolly"
                ][i % 6],

                Password: Crypto.randomBytes(32).toString("hex"),

                Hobbies: [
                    ["Sleep", "Programming"],
                    ["Eating", "Yoga"],
                    ["Skating"]
                ][Math.round(Math.random() * 2)]
            });
        }

        const TEnd = process.hrtime(TStart);
        const Time = TEnd[0] + (TEnd[1] / 1000000000);
        const DataSize = await GetSize(Connection);

        Close(Connection);

        process.stdout.write(
            Table.padEnd(10, " ").slice(Table.length) +
            CLI.green(`Created ${CLI.bold(DataSize)} entries `) +
            CLI.white(`(${Time.toFixed(3)}s)\n`)
        );
    }
}

const DatabaseTimingSpecs = new Map();

async function FetchBenchmark (Test, TableFetch, DoTheStuff, Close) {
    process.stdout.write(CLI.blue(`\n\n${Test}\n`));
    DatabaseTimingSpecs.set(Test, {});

    for (const [Table, Size] of Tables) {
        const {Connection, Indexes} = await TableFetch(Table);
        const TStart = process.hrtime();

        for (let i = 0; i < 1000000; i++) {
            const Id = Indexes[Math.round(Math.random() * (Indexes.length - 1))];
            await DoTheStuff(Connection, Id);
        }

        const TEnd = process.hrtime(TStart);
        const Time = TEnd[0] + (TEnd[1] / 1000000000);
        DatabaseTimingSpecs.get(Test)[Table] = {Table, Time, Size};

        Close(Connection);
    }

    const Current = DatabaseTimingSpecs.get(Test);

    for (const Table in Current)
    process.stdout.write(CLI.white(
        `\n· ${CLI.bold(Table.padEnd(15))}` +
        CLI.green.bold(`${Current[Table].Time.toFixed(3)}s`)
    ));
}


(async () => {
    process.stdout.write(CLI.white("Creating the necessary tables..."));

    await Populate("QDB", "data/qdb.qdb", Table => {
        return new QDB.Connection("data/qdb.qdb", {
            Table
        });
    },
        (Connection, Id, Document) => Connection.Set(Id, Document),
        Connection => Connection.Size,
        Connection => Connection.Disconnect()
    );

    await Populate("Enmap", "data/enmap.sqlite", async Table => {
        const MyEnmap = new Enm(Table);

        await MyEnmap.defer;
        return MyEnmap;
    },
        (Enmap, Id, Document) => Enmap.set(Id, Document),
        Enmap => Enmap.size,
        Enmap => Enmap.close()
    );
    
    await Populate("Josh", "data/josh.sqlite", async Table => {
        const MyJosh = new Jsh({
            name: Table,
            provider: require("@joshdb/sqlite")
        });

        await MyJosh.defer;
        return MyJosh;
    },
        async (Josh, Id, Document) => await Josh.set(Id, Document),
        async Josh => await Josh.size,
        () => {}
    );


    process.stdout.write(CLI.white("\nStarting universal benchmarks...\n"));

    await FetchBenchmark("QDB", Table => {
        const Connection = new QDB.Connection("data/qdb.qdb", {
            Table
        });
        
        const Indexes = Connection.Indexes;
        return {Connection, Indexes};
    },
        (Connection, Id) => Connection.Fetch(Id),
        Connection => Connection.Disconnect()
    );

    await FetchBenchmark("Enmap", async Table => {
        const MyEnmap = new Enm({
            name: Table,
            fetchAll: false
        });

        await MyEnmap.defer;

        return {
            Connection: MyEnmap,
            Indexes: MyEnmap.indexes
        };
    },
        (Enmap, Id) => Enmap.get(Id),
        Enmap => Enmap.close()
    );
    
    await FetchBenchmark("Josh", async Table => {
        const MyJosh = new Jsh({
            name: Table,
            provider: require("@joshdb/sqlite")
        });

        await MyJosh.defer;

        return {
            Connection: MyJosh,
            Indexes: await MyJosh.keys
        };
    },
        async (Josh, Id) => await Josh.get(Id),
        () => {}
    );
})();
