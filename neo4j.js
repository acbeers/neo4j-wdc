
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
        if(this._data) callback(this._data);

        console.log("running query");

        var connectionData = JSON.parse(tableau.connectionData);
        var queries = connectionData.queries;
        var labels = connectionData.labels;
        var statements = queries.map(function (x) { return {statement: x }});
        statements = statements.concat(labels.map(function (x) { return {statement: "match (node:"+x+") return node"} }));
        var data = JSON.stringify({statements: statements});
        console.log(data);

        var self = this;
        function doit(data)
        {
            this._data = data;
            callback(data);
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

        console.log(results);
        console.log(rows[0].row[0]);
        if(typeof(rows[0].row[0]) == "object") {
            cols = Object.keys(rows[0].row[0]);  // Odd.  The row is an array of a single object?
            isObj = true;
        }
        console.log(isObj);

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

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        console.log("getting schema");

        this._runQuery(function(data) {

            // FIXME: Assume that we are getting a single set of results.  I assume multiple sets of
            //        results could be exposed as multiple tables.
            var results = tableWrapper(data.results[0]);

            // Go through each column, look at first N data values to determine types.

            var numRowsToTest = 10;
            var cols = results.columns.map(function (x,i) {
                console.log(x);
                // Look at first N rows 
                var vals = results.slice(0,10).map(function (row) { return row[i] });

                // Are these values numbers?
                // FIXME: Need to add the case where we are returning a node, where each row is a dictionary.
                var test = vals.map(function (x) { return parseFloat(x) === x });
                var isNumber = test.reduce(function (x,y) { return x&&y },true);
                var dt = isNumber ? tableau.dataTypeEnum.float : tableau.dataTypeEnum.string;
                return {id: x, alias: x, dataType: dt };
            });

            var tableInfo = {
                id: "test",
                alias: "test",
                columns: cols
            };

            schemaCallback([tableInfo]);            
        });
    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {
        var mag = 0,
            title = "",
            url = "",
            lat = 0,
            lon = 0;

        this._runQuery(function (data) {
            var tableData = [];
            var results = tableWrapper(data.results[0]);
            var columns = results.columns;
            for (var i = 0, len = results.length; i < len; i++) 
            {
                tableData.push(results.row(i));
            }     
            table.appendRows(tableData);
            doneCallback();       
        });
    };

    function showinfo(data) {
        var cont = $("#label_list");

        data.forEach(function (elt,i) {
            cont.append($('<div><input name="labels" type=checkbox id='+i+' value="'+elt+'">' + elt + "</div>"));
        });
        $("#data").show();
    }
    function getinfo(neo) {
        $("#login").hide();
        $("#loggedin").show();
        $("#data").hide();

        neo.getNodeLabels(showinfo, function () { console.log("Can't show info from server."); });
    }

    function parseConnectionInfo()
    {
        var cd = {server:null, queries:[], labels:[]};
        if(tableau.connectionData)
            cd = JSON.parse(tableau.connectionData);
        return cd;
    }


    // This seems like a sneaky way of logging in
    myConnector.login = function () {
        console.log("login");
        var user     = $("#username").val().trim();
        var password = $("#password").val().trim();
        var server   = $("#serverurl").val().trim();

        var neo = Neo4J(server,user,password);

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
        console.log(labels);

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
