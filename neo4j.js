
// Things to work on:
// - break up into more testable pieces.  Hack time is over :)
// - the "labels" thing is rough.  We could try to compose a query that explicitly gathers the property names, 
//   then issues the right query.  Rather then asking for entire nodes, then re-parsing the JSON that comes back
// - If I do the above, I could then issue EXPLAIN queries to get back column names, making getSchema much cheaper
//   than it is today, and eliminating the need for caching between getSchema() and getData().  But, then
//   I'd have to just return strings, since EXPLAIN just returns names, not data or types.

var Neo4J = function (server,user,password) {

    if(server && server.slice(0,5) != "http:")
        server = "http://"+server+":7474";

    var baseUrl = server + "/db/data/";

    function settings(url,cb,eb) {
        return {
            url: url,
            success: cb,
            error: eb,
            contentType: "application/json",
            beforeSend: function(xhr){
                xhr.setRequestHeader("Authorization",
                    "Basic " + btoa(user + ":" + password));
            },
        }
    }

    function testConnected(callback,errback) {
        $.get(settings(baseUrl,callback,errback));
    }

    function getNodeLabels(callback,errback) {
        $.get(settings(baseUrl+"labels",callback,errback));        
    }

    return { 
        testConnected: testConnected,
        getNodeLabels: getNodeLabels
    };
};


(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // FIXME:  move most of this into the Neo4J object, above.
    myConnector._runQuery = function (callback) {

        var connectionData = JSON.parse(tableau.connectionData);
        var queries = connectionData.queries;
        var labels = connectionData.labels;
        var statements = queries.map(function (x) { return {statement: x }});
        statements = statements.concat(labels.map(function (x) { return {statement: "match (node:"+x+") return node"} }));
        var data = JSON.stringify({statements: statements});
    
        // Uhoh, the statements don't get eexecuted in order!!
        var ids = [];
        ids = ids.concat(queries.map(function (x,i) { return "Query " + (i+1)}));
        ids = ids.concat(labels);

        if(this._data) {
            callback(this._data,ids);
            return;
        }
        console.log("running query");

        var self = this;
        function doit(data)
        {
            self._data = data;
            callback(data,ids);
        }

        var server = connectionData.server;
        if(server.slice(0,5) != "http:")
            server = "http://"+server+":7474";

        var settings = {
            url: server+"/db/data/transaction/commit",
            contentType: "application/json",
            data: data,
            username: tableau.username,
            password: tableau.password,
            beforeSend: function(xhr){
                xhr.setRequestHeader("Authorization",
                    "Basic " + btoa("neo4j" + ":" + "graphs4me"));
            },
            success: doit
        }
        console.log("posting");
        $.post(settings);  
    };

    // A little wrapper around the rows that lets us refer to named columns
    // whether the rows are arrays or objects.
    //
    var tableWrapper = function (results) {

        var cols = results.columns;
        var rows = results.data;
        var isObj = false;

        if(typeof(rows[0].row[0]) == "object") {
            cols = Object.keys(rows[0].row[0]);  // Odd.  The row is an array of a single object?
            isObj = true;
        }

        function row(i) {
            if(! isObj) {
                var o = {};
                cols.forEach(function (x,j) { o[x] = rows[i].row[j] });
                return o;
            } else {
                // The rows are already objects.
                return rows[i].row[0]; // Odd.  The row is an array of a single object?               
            }
        }

        function slice(a,b) {
            var res = [];
            if(b > rows.length)
                b=rows.length;
            for(var i=a ; i<b ; i++) {
                res.push(row(i))
            }
            return res;
        }

        return {
            row: row,
            columns: cols,
            slice: slice,
            length: rows.length
        }
    };

    // FIXME:  Note that this is brittle, as it could CAUSE name collisions
    //         e.g. when h.name and h_name are both returned
    //
    myConnector.fixColumnName = function(cn) {
        return cn.replace(".","_");
    }

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        console.log("getting schema");
        var connector=this;

        this._runQuery(function(data,ids) {

            var schemas = [];
            for(var i=0 ; i<data.results.length ; i++)
            {
                var results = tableWrapper(data.results[i]);

                // Go through each column, look at first N data values to determine types.

                var numRowsToTest = 10;
                var cols = results.columns.map(function (x,i) {
                    // Look at first N rows 
                    var vals = results.slice(0,10).map(function (row) { return row[i] });

                    // Are these values numbers?
                    // FIXME: Need to add the case where we are returning a node, where each row is a dictionary.
                    var test = vals.map(function (x) { return parseFloat(x) === x });
                    var isNumber = test.reduce(function (x,y) { return x&&y },true);
                    var dt = isNumber ? tableau.dataTypeEnum.float : tableau.dataTypeEnum.string;
                    return {id: connector.fixColumnName(x), alias: x, dataType: dt };
                });

                var tableInfo = {
                    id: i.toString(),
                    alias: ids[i],
                    columns: cols
                };
                schemas.push(tableInfo);
            }

            schemaCallback(schemas);
        });
    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {
        var connector = this;

        this._runQuery(function (data) {
            var tableData = [];
            var results = tableWrapper(data.results[table.tableInfo.id]);
            var columns = results.columns;
            for (var i = 0, len = results.length; i < len; i++) 
            {
                var row = results.row(i);
                var newrow = {};
                columns.forEach(function (x) { newrow[connector.fixColumnName(x)] = row[x] })

                tableData.push(newrow);
            }     
            table.appendRows(tableData);
            doneCallback();       
        });
    };

    // Show the labels returned from the Server.
    // This will also select those that are present in the connection information.
    //
    function showinfo(data) {
        var cont = $("#label_list");

        data.forEach(function (elt,i) {
            cont.append($('<div><input name="labels" type=checkbox id='+i+' value="'+elt+'">' + elt + "</div>"));
        });
        $("#data").show();

        // Initiailize them
        // FIXME: This code is repeated below.
        var connectionData = {server:null, labels:[]};
        if(tableau.connectionData)
            connectionData = JSON.parse(tableau.connectionData);
        connectionData.labels.forEach(function (label) {
            var e = $('input[value="'+label+'"]');
            if(e) e.prop("checked",true);
        });

        $("#query").val(connectionData.queries[0]);
    }

    // Fetch information (node labels) from the Neo4J Server, calling showinfo() when done.
    function getinfo(neo) {
        $("#login").hide();
        $("#loggedin").show();
        $("#data").hide();

        neo.getNodeLabels(showinfo, function () { console.log("Can't show info from server."); });
    }

    // FIXME: This is currently unused, but should be how I get connection information.
    function parseConnectionInfo()
    {
        var cd = {server:null, queries:[], labels:[]};
        if(tableau.connectionData)
            cd = JSON.parse(tableau.connectionData);
        return cd;
    }


    // This seems like a sneaky way of logging in
    myConnector.login = function () {
        var user     = $("#username").val().trim();
        var password = $("#password").val().trim();
        var server   = $("#serverurl").val().trim();

        var neo = Neo4J(server,user,password);

        // FIXME:  Handle these errors properly - display some sort of message.
        function failure() {
            console.log("failed");
        }

        neo.testConnected(function () { getinfo(neo) } ,failure);
    }

    // Initialize
    myConnector.init = function(initCallback) {
        tableau.authType = tableau.authTypeEnum.basic;

        var user = tableau.username;
        var password = tableau.password;
        var connectionData = {server:null};
        if(tableau.connectionData)
            connectionData = JSON.parse(tableau.connectionData);
        var server = connectionData.server;

        $("#username").val(user);
        $("#password").val(password);
        $("#serverurl").val(server);

        var neo = Neo4J(server,user,password);

        function connected(data) {
            $("#login").hide();
            $("#loggedin").show();
            $("#data").hide();
            getinfo(neo);

            $("#query").val(connectionData.queries[0]);
        }
        function notConnected() {
            // Show the login UI.
            $("#login").show();
            $("#loggedin").hide();
            $("#data").hide();
            $("#loginButton").click(myConnector.login);
        }

        neo.testConnected(connected,notConnected);
        initCallback();
    };

    setupConnector = function() {
        var queryString = $('#query').val().trim();
        var username = $("#username").val().trim();
        var password = $("#password").val().trim();
        var server = $("#serverurl").val().trim();
        var labels = $("input[name=labels]:checked").map(function () { return this.value }).get();

        tableau.username = username;
        tableau.password = password;
        tableau.authType = tableau.authTypeEnum.basic;
        var queries = [];
        if(queryString.length > 0)
            queries.push(queryString);
        var cd = {
            server: server,
            queries: queries,
            labels: labels
        }
        tableau.connectionData = JSON.stringify(cd);
        tableau.connectionName = 'Neo4J Query'; 
        tableau.submit();
    };

    tableau.registerConnector(myConnector);

    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
            setupConnector();
        });
    });
})();
