<?php

namespace li3_xlsx\extensions\adapter\data\source\file;

use lithium\core\Libraries;
use lithium\core\NetworkException;
use lithium\util\Inflector;
use Exception;
use PhpOffice\PhpSpreadsheet\IOFactory;

/**
 * A data source adapter for querying Excel spreadsheets. Uses the
 * Sqlite3 adapter to act as a bridge for providing full read feature
 * set in Li3. For `database` configuration, ':memory:' is preferred,
 * as the Sqlite3 data store should be entirely temporary. Performance
 * is improved by cutting down on file read/write queries.
 */
class Xlsx extends \lithium\data\source\database\adapter\Sqlite3 {
    
    /**
     * Constructor method. Merges configurations supplied
     * by `Connections::add()`.
     *
     * @param array $config    The configuration array
     */
    public function __construct(array $config = []) {
        $resources = Libraries::get(true, 'resources');
        $seed      = md5(time());
        $tmp       = $resources . '/tmp/xlsx/' . $seed;
        $db        = $resources . '/data/' . $seed . '.sqlite';
        
        $defaults = [
            'files'       => [],
            'database'    => $db,
            'destroy'     => false
        ];
        $config += $defaults;
        
        parent::__construct($config);
    }
    
    /**
     * Connects to the specific Excel files and translates the data
     * for inclusion in Sqlite3 store.
     *
     * @return boolean    Returns `true` if the files exist
     */
    public function connect() {
        $this->_isConnected = false;
        $db    = $this->_config['database'];
        $files = $this->_config['files'];
        
        try {
            $files = is_array($files) ? $files : [$files];
            
            foreach ($files as $file) {
                if (!file_exists($file)) {
                    $this->_isConnected = false;
                    break;
                }
                $this->_isConnected = true;
            }
        } catch (Exception $e) {
            $msg = 'Could not connect to the Excel worksheet(s).';
            throw new NetworkException($msg, 503, $e);
        }
        
        //Populate Sqlite3 db with Excel data
        $xlsx = IOFactory::load(current($files)); //todo: iterate through $files and merge data into single array
    	$sheets = $xlsx->getSheetCount();
    	$db = $result = $schemas = [];
    	$types = ['s' => 'string', 'b' => 'integer', 'n' => 'integer', 'f' => 'integer'];
    	
    	for ($i = 0; $i < $sheets; $i++) {
        	$sheet = $xlsx->getSheet($i);
        	$title = strtolower($sheet->getTitle());
        	$maxCol = $sheet->getHighestDataColumn();
        	$cols = ord(strtolower($maxCol)) - 96;
            $range = sprintf('A1:%s%s', $maxCol, $sheet->getHighestDataRow());
            
            $db[$title] = $sheet->rangeToArray($range, null, true, false);
            
            for ($x = 1; $x <= $cols; $x++) {
                $index = strtoupper(chr($x + 96));
                $key = $db[$title][0][$x - 1];
                $type = $sheet->getCell($index . '2')->getDataType();
                $schemas[$title][$key] = $types[$type];
            }
    	}
    	
    	foreach ($db as $table => $rows) {
            $headers = $rows[0];
            foreach ($rows as $index => $row) {
                if ($index == 0) { //don't import header row
                    continue;
                }
                $result[$table][] = array_combine($headers, $row);
            }
    	}
        
        $this->_isConnected = parent::connect();
        $this->_initializeSQLiteDB($schemas);
        $this->_populateSQLiteDB($result);
        return $this->_isConnected;
    }
    
    /**
     * Disconnect from the Excel files.
     *
     * @return boolean    True
     */
    public function disconnect() {
        $this->_isConnected = false;
        $db  = $this->_config['database'];
        
        if ($this->_config['destroy'] == true && $db != ':memory:') {
            return $this->_removeDir($db);
        }
        unset($this->connection);
        return true;
    }
    
    /**
     * Returns the modified file stamp of the linked Excel file
     *
     * @param string $str Date-formatted string definition
     * @return string
     */
    public function modified($str = 'F j, Y - g:i A') {
        $files = $this->_config['files'];
        $files = is_array($files) ? $files : [$files];
        return date($str, filemtime(current($files)));
    }
    
    /**
     * Populates temporary SQLite3 database with Excel values
     * for full Li3 model functionality.
     *
     * @param array $rows
     * @return boolean true
     */
    protected function _populateSQLiteDB($rows) {
        $sql = "INSERT INTO %s(%s) VALUES(%s);";
        
        foreach ($rows as $table => $values) {
            foreach ($values as $row) {
                $keys = '`' . join('`, `', array_keys($row)) . '`';
                $vals = '"' . join('", "', $row) . '"';
                $cmd  = sprintf($sql, $table, $keys, $vals);
                $this->_execute($cmd);
            }
        }
        return true;
    }
    
    /**
     * Creates an SQLite3 database for temporary data handling
     * to provide full Li3 model functionality.
     *
     * @param array $schemas
     * @return boolean true
     */
    protected function _initializeSQLiteDB($schemas) {
        $sql = "CREATE TABLE IF NOT EXISTS %s(%s);";
        
        foreach ($schemas as $table => $fields) {
            $cols = [];
            $this->_execute('DROP TABLE IF EXISTS ' . $table);
            
            foreach ($fields as $field => $type) {
                $type = $type == 'string' ? 'VARCHAR(255)' : strtoupper($type);
                $cols[] = $field . ' ' . $type;
            }
            $cmd = sprintf($sql, $table, implode(',', $cols));
            $this->_execute($cmd);
        }
        return true;
    }
    
    /**
     * Cleans (deletes) temporary data source files.
     *
     * @param string $dir Path to delete
     * @param array $protected Paths to ignore
     * @return boolean
     */
    protected function _removeDir($dir, $protected = []) {
        $protected += [
            LITHIUM_APP_PATH,
            LITHIUM_LIBRARY_PATH,
            '/'
        ];
        
        if (!file_exists($dir)) {
            return true;
        }
        
        foreach ($protected as $path) {
            if ($dir == $path) {
                return false;
            }
        }
        system('rm -rf ' . $dir);
        return true;
    }
}

?>