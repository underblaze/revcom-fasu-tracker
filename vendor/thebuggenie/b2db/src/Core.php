<?php

    namespace b2db;

    use PDO,
        PDOException,
        ReflectionClass,
        ReflectionProperty;

    /**
     * B2DB Core class
     *
     * @author Daniel Andre Eikeland <zegenie@zegeniestudios.net>
     * @version 2.0
     * @license http://www.opensource.org/licenses/mozilla1.1.php Mozilla Public License 1.1 (MPL 1.1)
     * @package b2db
     * @subpackage core
     */

    /**
     * B2DB Core class
     *
     * @package b2db
     * @subpackage core
     */
    class Core
    {

        /**
         * PDO object
         *
         * @var \PDO
         */
        protected static $_db_connection = null;

        protected static $_db_host;

        protected static $_db_uname;

        protected static $_db_pwd;

        protected static $_db_name;

        protected static $_db_type;

        protected static $_db_port;

        protected static $_dsn;

        protected static $_tableprefix = '';

        protected static $_sqlhits = array();

        protected static $_sqltiming;

        protected static $_objectpopulationhits = array();

        protected static $_objectpopulationtiming;

        protected static $_objectpopulationcounts;

        protected static $_aliascnt = 0;

        protected static $_transaction_active = false;

        protected static $_tables = array();

        protected static $_debug_mode = true;

        protected static $_debug_logging = null;

        protected static $_cache_object = null;

        protected static $_cache_dir;

        protected static $_cached_entity_classes = array();

        protected static $_cached_table_classes = array();

        /**
         * Loads a table and adds it to the B2DBObject stack
         *
         * @param Table $table
         *
         * @return Table
         */
        public static function loadNewTable(Table $table)
        {
            self::$_tables['\\'.get_class($table)] = $table;
            return $table;
        }

        /**
         * Enable or disable debug mode
         *
         * @param boolean $debug_mode
         */
        public static function setDebugMode($debug_mode)
        {
            self::$_debug_mode = $debug_mode;
        }

        /**
         * Return whether or not debug mode is enabled
         *
         * @return boolean
         */
        public static function isDebugMode()
        {
            return self::$_debug_mode;
        }

        public static function getDebugTime()
        {
            return array_sum(explode(' ', microtime()));
        }

        public static function isDebugLoggingEnabled()
        {
            if (self::$_debug_logging === null)
                self::$_debug_logging = (self::isDebugMode() && class_exists('\\caspar\\core\\Logging'));

            return self::$_debug_logging;
        }

        /**
         * Add a table alias to alias counter
         *
         * @return integer
         */
        public static function addAlias()
        {
            return self::$_aliascnt++;
        }

        /**
         * Initialize B2DB and load related B2DB classes
         *
         * @param array $configuration [optional] Configuration to load
         * @param boolean $load_parameters [optional] whether to load connection parameters
         */
        public static function initialize($configuration = array(), $cache_object = null)
        {
            try {
                if (array_key_exists('dsn', $configuration) && $configuration['dsn'])
                    self::setDSN($configuration['dsn']);
                if (array_key_exists('driver', $configuration) && $configuration['driver'])
                    self::setDBtype($configuration['driver']);
                if (array_key_exists('hostname', $configuration) && $configuration['hostname'])
                    self::setHost($configuration['hostname']);
                if (array_key_exists('port', $configuration) && $configuration['port'])
                    self::setPort($configuration['port']);
                if (array_key_exists('username', $configuration) && $configuration['username'])
                    self::setUname($configuration['username']);
                if (array_key_exists('password', $configuration) && $configuration['password'])
                    self::setPasswd($configuration['password']);
                if (array_key_exists('database', $configuration) && $configuration['database'])
                    self::setDBname($configuration['database']);
                if (array_key_exists('tableprefix', $configuration) && $configuration['tableprefix'])
                    self::setTablePrefix($configuration['tableprefix']);
                if (array_key_exists('debug', $configuration))
                    self::setDebugMode((bool) $configuration['debug']);

                self::$_cache_object = $cache_object;
            } catch (\Exception $e) {
                throw $e;
            }
        }

        /**
         * Return true if B2DB is initialized with database name and username
         *
         * @return boolean
         */
        public static function isInitialized()
        {
            return (bool) (self::getDBtype() != '' && self::getUname() != '');
        }

        /**
         * Return the classpath to the B2DB engine files for the currently selected
         * db engine
         *
         * @return string
         */
        public static function getEngineClassPath()
        {
            return B2DB_BASEPATH . 'classes' . DIRECTORY_SEPARATOR;
        }

        /**
         * Store connection parameters
         *
         * @param string $bootstrap_location Where to save the connection parameters
         */
        public static function saveConnectionParameters($bootstrap_location)
        {
            $string = "b2db:\n";
            $string .= "    username: " . self::getUname() . "\n";
            $string .= "    password: \"" . self::getPasswd() . "\"\n";
            $string .= '    dsn: "' . self::getDSN() . "\"\n";
            $string .= "    tableprefix: '" . self::getTablePrefix() . "'\n";
            $string .= "    cacheclass: 'TBGCache'\n";
            $string .= "\n";
            try {
                if (file_put_contents($bootstrap_location, $string) === false) {
                    throw new Exception('Could not save the database connection details');
                }
            } catch (\Exception $e) {
                throw $e;
            }
        }

        /**
         * Returns the Table object
         *
         * @param string $tablename The table class name to load
         *
         * @return Table
         */
        public static function getTable($tablename)
        {
            if (!isset(self::$_tables[$tablename])) {
                self::loadNewTable(new $tablename());
            }
            if (!isset(self::$_tables[$tablename])) {
                throw new Exception('Table ' . $tablename . ' is not loaded');
            }
            return self::$_tables[$tablename];
        }

        protected static function getRelevantDebugBacktraceElement()
        {
            $trace = null;
            $backtrace = debug_backtrace();
            $reserved_names = array('Core.php', 'Saveable.php', 'Criteria.php', 'Criterion.php', 'Resultset.php', 'Row.php', 'Statement.php', 'Transaction.php', 'Criteria.php', 'B2DBCriterion.php', 'Row.php', 'Statement.php', 'Transaction.php', 'Table.php');

            foreach ($backtrace as $t) {
                if (isset($trace)) {
                    $trace['function'] = (isset($t['function'])) ? $t['function'] : 'unknown';
                    $trace['class'] = (isset($t['class'])) ? $t['class'] : 'unknown';
                    $trace['type'] = (isset($t['type'])) ? $t['type'] : 'unknown';
                    break;
                }
                if (!array_key_exists('file', $t)) continue;
                if (!\in_array(basename($t['file']), $reserved_names)) {
                    $trace = $t;
                    continue;
                }
            }

            return (!$trace) ? array('file' => 'unknown', 'line' => 'unknown', 'function' => 'unknown', 'class' => 'unknown', 'type' => 'unknown', 'args' => array()) : $trace;
        }

        /**
         * Register a new object population call (debug only)
         *
         * @param Resultset $resultset
         * @param array $classnames
         * @param mixed $pretime
         */
        public static function objectPopulationHit($num_classes, $classnames, $pretime)
        {
            if (!Core::isDebugMode() || !$num_classes)
                return;

            $time = Core::getDebugTime() - $pretime;
            $trace = self::getRelevantDebugBacktraceElement();

            self::$_objectpopulationhits[] = array('classnames' => $classnames, 'num_classes' => $num_classes, 'time' => $time, 'filename' => $trace['file'], 'line' => $trace['line'], 'function' => $trace['function'], 'class' => (isset($trace['class']) ? $trace['class'] : 'unknown'), 'type' => (isset($trace['type']) ? $trace['type'] : 'unknown'), 'arguments' => $trace['args']);
            self::$_objectpopulationcounts += $num_classes;
            self::$_objectpopulationtiming += $time;
        }

        /**
         * Register a new SQL call (debug only)
         *
         * @param Statement $statement
         * @param mixed $pretime
         */
        public static function sqlHit(Statement $statement, $pretime)
        {
            if (!Core::isDebugMode())
                return;

            $time = Core::getDebugTime() - $pretime;
            $sql = $statement->printSQL();
            $values = ($statement->getCriteria() instanceof Criteria) ? $statement->getCriteria()->getValues() : array();

            $trace = self::getRelevantDebugBacktraceElement();

            self::$_sqlhits[] = array('sql' => $sql, 'values' => implode(', ', $values), 'time' => $time, 'filename' => $trace['file'], 'line' => $trace['line'], 'function' => $trace['function'], 'class' => (isset($trace['class']) ? $trace['class'] : 'unknown'), 'type' => (isset($trace['type']) ? $trace['type'] : 'unknown'), 'arguments' => $trace['args']);
            self::$_sqltiming += $time;
        }

        /**
         * Get number of SQL calls
         *
         * @return integer
         */
        public static function getSQLHits()
        {
            return self::$_sqlhits;
        }

        public static function getSQLCount()
        {
            return count(self::$_sqlhits) + 1;
        }

        public static function getSQLTiming()
        {
            return self::$_sqltiming;
        }

        /**
         * Get number of object population calls
         *
         * @return integer
         */
        public static function getObjectPopulationHits()
        {
            return self::$_objectpopulationhits;
        }

        public static function getObjectPopulationCount()
        {
            return self::$_objectpopulationcounts;
        }

        public static function getObjectPopulationTiming()
        {
            return self::$_objectpopulationtiming;
        }

        /**
         * Returns PDO object
         *
         * @return \PDO
         */
        public static function getDBlink()
        {
            if (!self::$_db_connection instanceof PDO) {
                self::doConnect();
            }
            return self::$_db_connection;
        }

        /**
         * returns a PDO resultset
         *
         * @param string $sql
         */
        public static function simpleQuery($sql)
        {
            self::$_sqlhits++;
            try {
                $res = self::getDBLink()->query($sql);
            } catch (PDOException $e) {
                throw new Exception($e->getMessage());
            }
            return $res;
        }

        /**
         * Set the DSN
         *
         * @param string $dsn
         */
        public static function setDSN($dsn)
        {
            $dsn_details = parse_url($dsn);
            if (!is_array($dsn_details) || !array_key_exists('scheme', $dsn_details)) {
                throw new Exception('This does not look like a valid DSN - cannot read the database type');
            }
            try {
                self::setDBtype($dsn_details['scheme']);
                $details = explode(';', $dsn_details['path']);
                foreach ($details as $detail) {
                    $detail_info = explode('=', $detail);
                    if (count($detail_info) != 2) {
                        throw new B2DBException('This does not look like a valid DSN - cannot read the connection details');
                    }
                    switch ($detail_info[0]) {
                        case 'host':
                            self::setHost($detail_info[1]);
                            break;
                        case 'port':
                            self::setPort($detail_info[1]);
                            break;
                        case 'dbname':
                            self::setDBname($detail_info[1]);
                            break;
                    }
                }
            } catch (\Exception $e) {
                throw $e;
            }
            self::$_dsn = $dsn;
        }

        /**
         * Generate the DSN when needed
         */
        protected static function _generateDSN()
        {
            $dsn = self::getDBtype() . ":host=" . self::getHost();
            if (self::getPort()) {
                $dsn .= ';port=' . self::getPort();
            }
            $dsn .= ';dbname=' . self::getDBname();
            self::$_dsn = $dsn;
        }

        /**
         * Return current DSN
         *
         * @return string
         */
        public static function getDSN()
        {
            if (self::$_dsn === null) {
                self::_generateDSN();
            }
            return self::$_dsn;
        }

        /**
         * Set the database host
         *
         * @param string $host
         */
        public static function setHost($host)
        {
            self::$_db_host = $host;
        }

        /**
         * Return the database host
         *
         * @return string
         */
        public static function getHost()
        {
            return self::$_db_host;
        }

        /**
         * Return the database port
         *
         * @return integer
         */
        public static function getPort()
        {
            return self::$_db_port;
        }

        /**
         * Set the database port
         *
         * @param integer $port
         */
        public static function setPort($port)
        {
            self::$_db_port = $port;
        }

        /**
         * Set database username
         *
         * @param string $uname
         */
        public static function setUname($uname)
        {
            self::$_db_uname = $uname;
        }

        /**
         * Get database username
         *
         * @return string
         */
        public static function getUname()
        {
            return self::$_db_uname;
        }

        /**
         * Set the database table prefix
         *
         * @param string $prefix
         */
        public static function setTablePrefix($prefix)
        {
            self::$_tableprefix = $prefix;
        }

        /**
         * Get the database table prefix
         *
         * @return string
         */
        public static function getTablePrefix()
        {
            return self::$_tableprefix;
        }

        /**
         * Set the database password
         *
         * @param string $upwd
         */
        public static function setPasswd($upwd)
        {
            self::$_db_pwd = $upwd;
        }

        /**
         * Return the database password
         *
         * @return string
         */
        public static function getPasswd()
        {
            return self::$_db_pwd;
        }

        /**
         * Set the database name
         *
         * @param string $dbname
         */
        public static function setDBname($dbname)
        {
            self::$_db_name = $dbname;
            self::$_dsn = null;
        }

        /**
         * Get the database name
         *
         * @return string
         */
        public static function getDBname()
        {
            return self::$_db_name;
        }

        /**
         * Set the database type
         *
         * @param string $dbtype
         */
        public static function setDBtype($dbtype)
        {
            if (self::hasDBEngine($dbtype) == false) {
                throw new Exception('The selected database is not supported: "' . $dbtype . '".');
            }
            self::$_db_type = $dbtype;
        }

        /**
         * Get the database type
         *
         * @return string
         */
        public static function getDBtype()
        {
            if (!self::$_db_type && defined('B2DB_SQLTYPE')) {
                self::setDBtype(B2DB_SQLTYPE);
            }
            return self::$_db_type;
        }

        public static function hasDBtype()
        {
            return (bool) (self::getDBtype() != '');
        }

        /**
         * Try connecting to the database
         */
        public static function doConnect()
        {
            if (!\class_exists('\\PDO')) {
                throw new Exception('B2DB needs the PDO PHP libraries installed. See http://php.net/PDO for more information.');
            }
            try {
                $uname = self::getUname();
                $pwd = self::getPasswd();
                $dsn = self::getDSN();
                if (self::$_db_connection instanceof PDO) {
                    self::$_db_connection = null;
                }
                self::$_db_connection = new PDO($dsn, $uname, $pwd);
                if (!self::$_db_connection instanceof PDO) {
                    throw new Exception('Could not connect to the database, but not caught by PDO');
                }
		switch (self::getDBtype())
		{
		  case 'mysql':
		    self::getDBLink()->query('SET NAMES UTF8');
		    break;
		  case 'pgsql':
		    self::getDBlink()->query('set client_encoding to UTF8');
		    break;
		}
            } catch (PDOException $e) {
                throw new Exception("Could not connect to the database [" . $e->getMessage() . "], dsn: ".self::getDSN());
            } catch (Exception $e) {
                throw $e;
            }
        }

        /**
         * Create the specified database
         *
         * @param string $db_name
         */
        public static function createDatabase($db_name)
        {
            self::getDBLink()->query('create database ' . $db_name);
        }

        /**
         * Close the database connection
         */
        public static function closeDBLink()
        {
            self::$_db_connection = null;
        }

        public static function isConnected()
        {
            return (bool) (self::$_db_connection instanceof PDO);
        }

        public static function getCacheDir()
        {
            if (self::$_cache_dir === null) {
                $cache_dir = (defined('B2DB_CACHEPATH')) ? realpath(B2DB_CACHEPATH) : realpath(B2DB_BASEPATH . 'cache/');
                self::$_cache_dir = $cache_dir;
            }
            return self::$_cache_dir;
        }

        /**
         * Toggle the transaction state
         *
         * @param boolean $state
         */
        public static function setTransaction($state)
        {
            self::$_transaction_active = $state;
        }

        /**
         * Starts a new transaction
         */
        public static function startTransaction()
        {
            return new Transaction();
        }

        public static function isTransactionActive()
        {
            return (bool) self::$_transaction_active == Transaction::DB_TRANSACTION_STARTED;
        }

        /**
         * Get available DB drivers
         *
         * @return array
         */
        public static function getDBtypes()
        {
            $retarr = array();

            if (class_exists('\PDO')) {
                $retarr['mysql'] = 'MySQL';
                $retarr['pgsql'] = 'PostgreSQL';
                $retarr['mssql'] = 'Microsoft SQL Server';
                /*
                  $retarr['sqlite'] = 'SQLite';
                  $retarr['sybase'] = 'Sybase';
                  $retarr['dblib'] = 'DBLib';
                  $retarr['firebird'] = 'Firebird';
                  $retarr['ibm'] = 'IBM';
                  $retarr['oci'] = 'Oracle';
                 */
            } else {
                throw new Exception('You need to have PHP PDO installed to be able to use B2DB');
            }

            return $retarr;
        }

        /**
         * Whether a specific DB driver is supported
         *
         * @param string $driver
         *
         * @return boolean
         */
        public static function hasDBEngine($driver)
        {
            return array_key_exists($driver, self::getDBtypes());
        }

        protected static function storeCachedTableClass($classname)
        {
            $key = 'b2db_cache_' . $classname;
            self::$_cache_object->add($key, self::$_cached_table_classes[$classname]);
            self::$_cache_object->fileAdd($key, self::$_cached_table_classes[$classname]);
        }

        protected static function cacheTableClass($classname)
        {
            if (!\class_exists($classname)) {
                throw new Exception("The class '{$classname}' does not exist");
            }
            self::$_cached_table_classes[$classname] = array('entity' => null, 'name' => null, 'discriminator' => null);

            $reflection = new ReflectionClass($classname);
            $docblock = $reflection->getDocComment();
            $annotationset = new AnnotationSet($docblock);
            if (!$table_annotation = $annotationset->getAnnotation('Table')) {
                throw new Exception("The class '{$classname}' does not have a proper @Table annotation");
            }
            $table_name = $table_annotation->getProperty('name');

            if ($entity_annotation = $annotationset->getAnnotation('Entity'))
                self::$_cached_table_classes[$classname]['entity'] = $entity_annotation->getProperty('class');

            if ($entities_annotation = $annotationset->getAnnotation('Entities')) {
                $details = array('identifier' => $entities_annotation->getProperty('identifier'), 'classes' => $annotationset->getAnnotation('SubClasses')->getProperties());
                self::$_cached_table_classes[$classname]['entities'] = $details;
            }

            if ($discriminator_annotation = $annotationset->getAnnotation('Discriminator')) {
                $details = array('column' => "{$table_name}." . $discriminator_annotation->getProperty('column'), 'discriminators' => $annotationset->getAnnotation('Discriminators')->getProperties());
                self::$_cached_table_classes[$classname]['discriminator'] = $details;
            }

            if (!$table_annotation->hasProperty('name')) {
                throw new Exception("The class @Table annotation in '{$classname}' is missing required 'name' property");
            }

            self::$_cached_table_classes[$classname]['name'] = $table_name;

            if (!self::$_debug_mode) self::storeCachedTableClass($classname);
        }

        protected static function storeCachedEntityClass($classname)
        {
            $key = 'b2db_cache_' . $classname;
            self::$_cache_object->add($key, self::$_cached_entity_classes[$classname]);
            self::$_cache_object->fileAdd($key, self::$_cached_entity_classes[$classname]);
        }

        protected static function cacheEntityClass($classname, $reflection_classname = null)
        {
            $rc_name = ($reflection_classname !== null) ? $reflection_classname : $classname;
            $reflection = new ReflectionClass($rc_name);
            $annotationset = new AnnotationSet($reflection->getDocComment());

            if ($reflection_classname === null) {
                self::$_cached_entity_classes[$classname] = array('columns' => array(), 'relations' => array(), 'foreign_columns' => array(),);
                if (!$annotation = $annotationset->getAnnotation('Table')) {
                    throw new Exception("The class '{$classname}' is missing a valid @Table annotation");
                } else {
                    $tablename = $annotation->getProperty('name');
                }
                if (!\class_exists($tablename)) {
                    throw new Exception("The class table class '{$tablename}' for class '{$classname}' does not exist");
                }
                self::$_cached_entity_classes[$classname]['table'] = $tablename;
                self::_populateCachedTableClassFiles($tablename);
                if (($re = $reflection->getExtension()) && $classnames = $re->getClassNames()) {
                    foreach ($classnames as $extends_classname) {
                        self::cacheEntityClass($classname, $extends_classname);
                    }
                }
            }
            if (!\array_key_exists('name', self::$_cached_table_classes[self::$_cached_entity_classes[$classname]['table']])) {
                throw new Exception("The class @Table annotation in '" . self::$_cached_entity_classes[$classname]['table'] . "' is missing required 'name' property");
            }
            $column_prefix = self::$_cached_table_classes[self::$_cached_entity_classes[$classname]['table']]['name'] . '.';

            foreach ($reflection->getProperties() as $property) {
                $annotationset = new AnnotationSet($property->getDocComment());
                if ($annotationset->hasAnnotations()) {
                    $property_name = $property->getName();
                    if ($column_annotation = $annotationset->getAnnotation('Column')) {
                        $column_name = $column_prefix . (($column_annotation->hasProperty('name')) ? $column_annotation->getProperty('name') : substr($property_name, 1));

                        $column = array('property' => $property_name, 'name' => $column_name, 'type' => $column_annotation->getProperty('type'));

                        $column['not_null'] = ($column_annotation->hasProperty('not_null')) ? $column_annotation->getProperty('not_null') : false;

                        if ($column_annotation->hasProperty('default_value')) $column['default_value'] = $column_annotation->getProperty('default_value');
                        if ($column_annotation->hasProperty('length')) $column['length'] = $column_annotation->getProperty('length');

                        switch ($column['type']) {
                            case 'serializable':
                                $column['type'] = 'serializable';
                                break;
                            case 'varchar':
                            case 'string':
                                $column['type'] = 'varchar';
                                break;
                            case 'float':
                                $column['precision'] = ($column_annotation->hasProperty('precision')) ? $column_annotation->getProperty('precision') : 2;
                            case 'integer':
                                $column['auto_inc'] = ($column_annotation->hasProperty('auto_increment')) ? $column_annotation->getProperty('auto_increment') : false;
                                $column['unsigned'] = ($column_annotation->hasProperty('unsigned')) ? $column_annotation->getProperty('unsigned') : false;
                                if (!isset($column['length'])) $column['length'] = 10;
                                if ($column['type'] != 'float'&& !isset($column['default_value'])) $column['default_value'] = 0;
                                break;
                        }
                        self::$_cached_entity_classes[$classname]['columns'][$column_name] = $column;
                        if ($annotationset->hasAnnotation('Id')) {
                            self::$_cached_entity_classes[$classname]['id_column'] = $column_name;
                        }
                    }
                    if ($annotation = $annotationset->getAnnotation('Relates')) {
                        $value = $annotation->getProperty('class');
                        $collection = (bool) $annotation->getProperty('collection');
                        $manytomany = (bool) $annotation->getProperty('manytomany');
                        $joinclass = $annotation->getProperty('joinclass');
                        $foreign_column = $annotation->getProperty('foreign_column');
                        $orderby = $annotation->getProperty('orderby');
                        $f_column = $annotation->getProperty('column');
                        self::$_cached_entity_classes[$classname]['relations'][$property_name] = array('collection' => $collection, 'property' => $property_name, 'foreign_column' => $foreign_column, 'manytomany' => $manytomany, 'joinclass' => $joinclass, 'class' => $annotation->getProperty('class'), 'column' => $f_column, 'orderby' => $orderby);
                        if (!$collection) {
                            if (!$column_annotation || !isset($column)) {
                                throw new Exception("The property '{$property_name}' in class '{$classname}' is missing an @Column annotation, or is improperly marked as not being a collection");
                            }
                            $column_name = $column_prefix . (($annotation->hasProperty('name')) ? $annotation->getProperty('name') : substr($property_name, 1));
                            $column['class'] = self::getCachedB2DBTableClass($value);
                            $column['key'] = ($annotation->hasProperty('key')) ? $annotation->getProperty('key') : null;
                            self::$_cached_entity_classes[$classname]['foreign_columns'][$column_name] = $column;
                        }
                    }
                }
            }

            if (!self::$_debug_mode) self::storeCachedEntityClass($classname);
        }

        protected static function _populateCachedClassFiles($classname)
        {
            if (!array_key_exists($classname, self::$_cached_entity_classes)) {
                $entity_key = 'b2db_cache_' . $classname;
                if (self::$_cache_object && self::$_cache_object->has($entity_key)) {
                    self::$_cached_entity_classes[$classname] = self::$_cache_object->get($entity_key);
                } elseif (self::$_cache_object && self::$_cache_object->fileHas($entity_key)) {
                    self::$_cached_entity_classes[$classname] = self::$_cache_object->fileGet($entity_key);
                } else {
                    self::cacheEntityClass($classname);
                }
            }
        }

        protected static function _populateCachedTableClassFiles($classname)
        {
            if (!array_key_exists($classname, self::$_cached_table_classes)) {
                $key = 'b2db_cache_' . $classname;
                if (self::$_cache_object && self::$_cache_object->has($key)) {
                    self::$_cached_table_classes[$classname] = self::$_cache_object->get($key);
                } elseif (self::$_cache_object && self::$_cache_object->fileHas($key)) {
                    self::$_cached_table_classes[$classname] = self::$_cache_object->fileGet($key);
                } else {
                    self::cacheTableClass($classname);
                }
            }
        }

        public static function getTableDetails($classname)
        {
            $table = $classname::getTable();
            if ($table instanceof Table) {
                self::_populateCachedTableClassFiles($classname);
                return array('columns' => $table->getColumns(),
                    'foreign_columns' => $table->getForeignColumns(),
                    'id' => $table->getIdColumn(),
                    'discriminator' => self::$_cached_table_classes[$classname]['discriminator'],
                    'name' => self::$_cached_table_classes[$classname]['name']
                );
            }
        }

        public static function getCachedTableDetails($classname)
        {
            self::_populateCachedClassFiles($classname);
            if (array_key_exists($classname, self::$_cached_entity_classes) && array_key_exists('columns', self::$_cached_entity_classes[$classname])) {
                if (!array_key_exists('id_column', self::$_cached_entity_classes[$classname])) {
                    throw new Exception('Cannot find an id column for this table.');
                }
                return array('columns' => self::$_cached_entity_classes[$classname]['columns'],
                    'foreign_columns' => self::$_cached_entity_classes[$classname]['foreign_columns'],
                    'id' => self::$_cached_entity_classes[$classname]['id_column'],
                    'discriminator' => self::$_cached_table_classes[self::$_cached_entity_classes[$classname]['table']]['discriminator'],
                    'name' => self::$_cached_table_classes[self::$_cached_entity_classes[$classname]['table']]['name']
                );
            }
            return null;
        }

        protected static function _getCachedEntityDetail($classname, $key, $detail = null)
        {
            self::_populateCachedClassFiles($classname);
            if (array_key_exists($classname, self::$_cached_entity_classes)) {
                if (!array_key_exists($key, self::$_cached_entity_classes[$classname])) {
                    if ($key == 'table') throw new Exception("The class '{$classname}' is missing a valid @Table annotation");
                } elseif ($detail === null) {
                    return self::$_cached_entity_classes[$classname][$key];
                } elseif (isset(self::$_cached_entity_classes[$classname][$key][$detail])) {
                    return self::$_cached_entity_classes[$classname][$key][$detail];
                }
            }
            return null;
        }

        protected static function _getCachedTableDetail($classname, $detail)
        {
            self::_populateCachedTableClassFiles($classname);
            if (array_key_exists($classname, self::$_cached_table_classes)) {
                if (!array_key_exists($detail, self::$_cached_table_classes[$classname])) {
                    if ($detail == 'entity') throw new Exception("The class '{$classname}' is missing a valid @Entity annotation");
                } else {
                    return self::$_cached_table_classes[$classname][$detail];
                }
            }
            return null;
        }

        public static function getCachedEntityRelationDetails($classname, $property)
        {
            return self::_getCachedEntityDetail($classname, 'relations', $property);
        }

        public static function getCachedColumnDetails($classname, $column)
        {
            return self::_getCachedEntityDetail($classname, 'columns', $column);
        }

        public static function getCachedColumnPropertyName($classname, $column)
        {
            $column_details = self::getCachedColumnDetails($classname, $column);
            return (is_array($column_details)) ? $column_details['property'] : null;
        }

        public static function getCachedB2DBTableClass($classname)
        {
            return self::_getCachedEntityDetail($classname, 'table');
        }

        public static function getCachedTableEntityClasses($classname)
        {
            return self::_getCachedTableDetail($classname, 'entities');
        }

        public static function getCachedTableEntityClass($classname)
        {
            return self::_getCachedTableDetail($classname, 'entity');
        }

    }
