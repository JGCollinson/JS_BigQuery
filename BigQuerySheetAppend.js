function runQuery() {
    // Replace this value with the project ID listed in the Google
    // Cloud Platform project.
    var projectId = 'bks9992003lxlleeol9902-0000s';
    var request = {
      query: 'SELECT ProdASIN FROM ( SELECT REGEXP_EXTRACT(hits.eventInfo.eventAction,r\'(\\bB[0-9A-Z]+)\') AS ProdASIN FROM TABLE_DATE_RANGE([125604721.ga_sessions_], TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, \'DAY\')), TIMESTAMP(DATE_ADD(CURRENT_DATE(), -1, \'DAY\'))) WHERE hits.eventInfo.eventAction CONTAINS \'www.amazon.com\') WHERE ProdASIN IS NOT NULL;'  
    };
    var queryResults = BigQuery.Jobs.query(request, projectId);
    var jobId = queryResults.jobReference.jobId;
  
    // Check on status of the Query Job.
    var sleepTimeMs = 500;
    while (!queryResults.jobComplete) {
      Utilities.sleep(sleepTimeMs);
      sleepTimeMs *= 2;
      queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
    }
  
    // Get all the rows of results.
    var rows = queryResults.rows;
    while (queryResults.pageToken) {
      queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
        pageToken: queryResults.pageToken
      });
      rows = rows.concat(queryResults.rows);
    }
  
    if (rows) {
      var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
      var sheet = spreadsheet.getActiveSheet();
      var trash = sheet.getRange("A:A");
      
      trash.clear();
      // Append the headers.
      var headers = queryResults.schema.fields.map(function(field) {
        return field.name;
      });
      sheet.appendRow(headers);
  
      // Append the results. 
      var data = new Array(rows.length);
      for (var i = 0; i < rows.length; i++) {
        var cols = rows[i].f;
        data[i] = new Array(cols.length);
        for (var j = 0; j < cols.length; j++) {
          data[i][j] = cols[j].v;
        }
      }
      sheet.getRange(2, 1, rows.length, headers.length).setValues(data);
  
      Logger.log('Results spreadsheet created: %s',
          spreadsheet.getUrl());
    } else {
      Logger.log('No rows returned.');
    }
  }