<?php
    $db = new SQLite3('/tmp/mysqlitedb.db');
    //print "hello";
    $table_exists = $db->query("SELECT COUNT(*) as count FROM sqlite_master WHERE type='table' AND name='".$db->escapeString($_GET['tube'])."' LIMIT 1");
    $table_exists = ($table_exists->fetchArray(SQLITE3_ASSOC)['count'] == 0) ? False : True;
    //var_dump($table_exists);
    //var_dump($_SERVER['REQUEST_METHOD']);
    switch($_SERVER['REQUEST_METHOD'])    {
        case 'POST':
            if($table_exists == False)    {
                $db->exec("CREATE TABLE IF NOT EXISTS ".$db->escapeString($_GET['tube'])." (ID INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT NOT NULL,state INTEGER NOT NULL,ts INTEGER,ttr INTERGER)");            
            }
            $write_conform = $db->exec("INSERT INTO ".$db->escapeString($_GET['tube'])." (value,state,ts) VALUES ('".$_POST['task']."', 0, ".time().");");
            if($write_conform == true)  {
                print json_encode(array('tube' => $_GET['tube'], 'id' => $db->lastInsertRowID()));
            } else {
                print json_encode(array('tube' => $_GET['tube'], 'id' => false));
            }
            break;
        case 'GET':
            if($table_exists == False)    {
                print json_encode(array('tube' => $_GET['tube'], 'tasks' => False));
            } else {
                //$count = (is_int($_GET['count']) && $_GET['count'] > 0) ? $_GET['count'] : 1;
                $db->exec("BEGIN EXCLUSIVE TRANSACTION");
                $task_q = $db->query("SELECT ID as id,value FROM ".$db->escapeString($_GET['tube'])." WHERE state=0 ORDER BY ID ASC LIMIT 1")->fetchArray(SQLITE3_ASSOC);
                if($task_q != False){
                    $ts = time();
                    $db->exec("UPDATE ".$db->escapeString($_GET['tube'])." SET state=1,ts=".$ts." WHERE ID=".$db->escapeString($task_q['id']));
                } else {
                    $task_q = array();
                }
                $db->exec("COMMIT");
                
                print json_encode(array('tube' => $_GET['tube'], 'tasks' => $task_q, 'ts' => $ts));
            }
            break;
        case 'DELETE':
            if($table_exists == True)    {
                if($_GET['ts']) {
                    $d_result = $db->exec("DELETE FROM ".$db->escapeString($_GET['tube'])." WHERE ID=".$db->escapeString($_GET['id'])." AND ts=".$db->escapeString($_GET['ts']));
                } else {
                    $d_result = $db->exec("DELETE FROM ".$db->escapeString($_GET['tube'])." WHERE ID=".$db->escapeString($_GET['id']));
                }
                if($d_result == True && $db->changes() > 0)   {
                    print json_encode(array('tube' => $_GET['tube'], 'deleted' => True));
                } else {
                    print json_encode(array('tube' => $_GET['tube'], 'deleted' => False));
                }
            }
            break;
    }
