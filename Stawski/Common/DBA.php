<?php
/**
 * @package Stawski
 * @subpackage Common
 * 
 * @version 0.1-3.12.2012
 */
namespace Stawski\Common;
/**
 * @class \Stawski\Common\DBA   Основной класс доступа к базам данных
 */
class DBA {
	private $schemes = [];
	private $links = [];

	private $_queries = [];

	static $instance;

	private $logs = true;
	private $debug = true;

	/**
	* Возвращает экземпляр класса DBA
	*
	* @return \Stawski\Common\DBA
	*/
	public static function instance_of() {
		if(self::$instance === NULL) {
			self::$instance = new DBA();
		}
                
		return self::$instance;
	}
	
	/**
	* Конструктор класса DBA
	*/	
	public function __construct() {
	}

	/**
	* Отключает логирование SQL запросов
	*
	* @return NULL
	*/
	public function no_logs() {
		$this->logs = false;
	}

	/**
	* Включает логирование запросов
	* 
	* @return NULL
	*/
	public function enable_logs() {
		$this->logs = true;	
	}

	/**
	* Отключает вывод отладочной информации
	*
	* @return NULL
	*/
	public function no_debug() {
		$this->debug = false;
	}

	/**
	* Включает вывод отладочной информации
	*
	* @return NULL
	*/
	public function enable_debug() {
		$this->debug = true;	
	}

	/**
	 * Подключается к серверу базы данных MySQL
	 *
         * @param string $id        идентификатор базы данных
         * @param string $host      хост базы данных
         * @param string $user      пользователь базы данных
         * @param string $pass      пароль для пользователя
         * @param string $db        имя базы данных
         * @param string $charset   кодировка
	 * @return boolean TRUE - если успешно и FALSE - если не удалось установить подключение
	*/
	public function connect($id, $host, $user, $pass, $db, $charset) {
		$this->links[$id] = mysqli_connect($host,$user,$pass,$db);
		$this->query_execute($id, 'SET CHARSET '.$charset);
		return TRUE;
	}

	/**
	* Регистрирует новую схему базы данных
	*
	* @param string $id     уникальный идентификатор схемы
	* @param array $schema  массив, содержащий описание схемы
	* @return boolean
	*/
	public function register_schema($id, $schema) {
		//TODO: Check valid
		$this->schemes[$id] = $schema;
		return TRUE;
	}

	/**
	* Выполняет SQL запрос
	*
	* @param string $db     идентификатор базы данных
	* @param string $query   SQL запрос
	* @return boolean
	*/
	public function query_execute($db, $query) {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}

		if($this->logs) { $this->_queries[] = $query; }
		return mysqli_query($this->links[$db], $query);
	}

        /**
         * Выполняется SQL запрос и возвращается число затронутых строк
         * 
         * @param string $db     идентификатор базы данных   
         * @param string $query  SQL запрос
         */
	public function query_affected($db, $query) {

	}

        /**
         * Выполняется SQL запрос и возвращается первую строку
         * 
         * @param string $db     идентификатор базы данных
         * @param string $query  SQL запрос
         */
	public function query_get_one($db, $query) {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}

		if($this->logs) { $this->_queries[] = $query; }

		if($res = mysqli_query($this->links[$db], $query)) {
			return mysqli_fetch_object($res);
		}

		return NULL;
	}

        /**
         * Выполняется SQL запрос и возвращается первую строку
         * 
         * @param string $db     идентификатор базы данных
         * @param string $query  SQL запрос
         * @return object
         */
	public function query_get_one_a($db, $query) {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}

		if($this->logs) { $this->_queries[] = $query; }

		if($res = mysqli_query($this->links[$db], $query)) {
			return mysqli_fetch_array($res, MYSQLI_ASSOC);
		}

		return NULL;
	}

        /**
         * Выполняется SQL запрос и возвращается объект с атрибутами, содержащими ссылки на объекты.
         * 
         * @param string $db     идентификатор базы данных
         * @param string $query  SQL запрос
         * @return object / NULL
         */
	public function query_get_multi($db, $query) {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}

		if($this->logs) { $this->_queries[] = $query; }

		$res = mysqli_query($this->links[$db], $query);
		
		if($res) {
			$items = mysqli_fetch_all($res, MYSQLI_ASSOC);
			$_res = [];
			foreach($items as $item) {
				$_res[] = (object) $item;
			}
			return $_res;
		}

		return NULL;
	}

        /**
         * Выполняется SQL запрос и возвращается массив, содержащий ссылки на объекты.
         * 
         * 
         * @param string $db     идентификатор базы данных
         * @param string $query  SQL запрос
         * @return array 
         */
	public function query_get_multi_a($db, $query) {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}

		if($this->logs) { $this->_queries[] = $query; }

		$res = mysqli_query($this->links[$db], $query);
		
		return mysqli_fetch_all($res, MYSQLI_ASSOC);
	}

        /**
         * Открывает транзакцию
         * 
         * @param string $db    идентификатор базы данных
         * @param string $id    идентификатор транзакции
         * @return boolean
         * @throws \Stawski\Common\DBADBNotFoundException
         */
	public function begin_transaction($db, $id) {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}
	}

        /**
         * Закрывает транзакцию и сохранаяет изменения
         * 
         * @param string $db    идентификатор базы данных
         * @param string $id    идентификатор транзакции, по умолчанию последнюю
         * @return boolean
         * @throws \Stawski\Common\DBADBNotFoundException
         */
	public function commit($db, $id = '__last__') {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}
	}

        /**
         * Закрывает транзакцию и отменяет изменения
         * 
         * @param string $db    идентификатор базы данных
         * @param string $id    идентификатор транзакции, по умолчанию последнюю
         * @return boolean
         * @throws \Stawski\Common\DBADBNotFoundException
         */
	public function rollback($db, $id = '__last__') {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}
	}

        /**
         * Устанавливает кодировку
         * 
         * @param string $db        идентификатор базы данных
         * @param string $charset   идентификатор кодировки, по умолчанию UTF8
         * @return boolean
         * @throws \Stawski\Common\DBADBNotFoundException
         */
	public function charset($db, $charset = 'UTF8') {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}
	}

        /**
         * Генерирует условие WHERE для идентификации уникального объекта
         * 
         * @param string $schema    идентификатор схемы
         * @param object $object    объект сущности
         * @return string
         */
	public function generate_identify($schema, $object) {
		$cases = array();
		foreach ($schema['item_identify_by'] as $field_id) {
			$cases[] = "`{$schema['table']}`.`{$schema['global_prefix']}{$field_id}` = '{$object->$field_id}'";
		}

		return implode(' AND ', $cases);
	}

        /**
         * Возвращает TRUE, если это поле объекта является виртуальным
         * 
         * @param string $field     идентификатор поля
         * @return boolean
         */
	public function is_virtual_field($field) {
		$virtual_typpes = ['concat', 'ref', 'optional'];
		return in_array($field['type'], $virtual_typpes);
	}

        /**
         * Генерирует список полей для установки при UPDATE
         * 
         * @param string $schema    идентификатор схемы
         * @param object $object    объект сущности
         * @param array  $fields    список полей, по умолчанию NULL
         * @return string
         */
	public function generate_seters($schema, $object, $fields = NULL) {
		$seters = array();
		$id_fields = array_flip($schema['item_identify_by']);
		$prefix = "`{$schema['table']}`.`{$schema['global_prefix']}";
		if($fields !== NULL) {
			foreach ($fields as $field_id) {
				if($this->is_virtual_field($schema['fields'][$field_id])) continue;

				if(!isset($id_fields[$field_id]) && isset($object->$field_id)) {
					$seters[] = "{$prefix}{$field_id}` = ".$this->_value($schema['fields'][$field_id], $object->$field_id);
				}
			}
		} else {
			foreach ($schema['fields'] as $field_id => $field) {
				if($this->is_virtual_field($field)) continue;

				if(!isset($id_fields[$field_id]) && isset($object->$field_id)) {
					$seters[] = "{$prefix}{$field_id}` = ".$this->_value($schema['fields'][$field_id], $object->$field_id);
				}
			}
		}
		if(count($seters) < 1) { return NULL; }
		return implode(', ', $seters);
	}

        /**
         * Возвращает объект сущности
         * 
         * @param object $obj   Объект сущности 
         * @param string $sql   Запрос
         * @return boolean
         * @throws \Stawski\Common\DBASchemaNotFoundException
         * @throws \Stawski\Common\DBADBNotFoundException
         * @throws \Stawski\Common\DBAItemNotFoundException
         */
	public function get_item_raw($obj, $sql) {
		list($entity_db, $entity_schema) = $obj->get_info();

		if(!isset($this->schemes[$entity_schema])) {
			throw new DBASchemaNotFoundException();
		}
		if(!isset($this->links[$entity_db])) {
			throw new DBADBNotFoundException('Database `'.$entity_db.'` not connected.');
		}

		$entity = $this->query_get_one($entity_db, $sql);
		$schema = $this->schemes[$entity_schema];

		if($entity) {
			foreach ($this->schemes[$entity_schema]['fields'] as $field => $value) {
				if($this->is_virtual_field($value)) continue;

				$obj->$field = $entity->{$this->schemes[$entity_schema]['global_prefix'].$field};
			}

			foreach ($this->schemes[$entity_schema]['fields'] as $field => $value) {
				if($value['type'] != 'concat') { continue; }
				$v = [];
				foreach ($value['concat'] as $f) {
					$v[] = $entity->{$this->schemes[$entity_schema]['global_prefix'].$f};
				}
				$obj->{$field} = implode(' ', $v);
			}

			$obj->make_snapshot();

			if(isset($schema['links']) && count($schema['links']) > 0) {
				foreach ($schema['links'] as $field => $link_nfo) {
								
					$value = $obj->{$link_nfo['by'][0]};

					if(!isset($this->schemes[$link_nfo['schema']])) {
						throw new DBASchemaNotFoundException();
					}

					$linked_schema = $this->schemes[$link_nfo['schema']];

					if($link_nfo['type'] === 'one-to-one') {
						$obj->$field = new $link_nfo['to']();
						$this->get_item_raw($obj->$field, "SELECT * FROM `{$linked_schema['table']}` WHERE `{$linked_schema['global_prefix']}{$link_nfo['by'][1]}` = '{$value}' LIMIT 1");
					} else {
						$obj->$field = new \stdClass();	
						$this->get_items_raw($obj->$field, $link_nfo['to'], "SELECT * FROM `{$linked_schema['table']}` WHERE `{$linked_schema['global_prefix']}{$link_nfo['by'][1]}` = '{$value}'");
					}
				}
			}

			return TRUE;
		}

		throw new DBAItemNotFoundException();
	}

        /**
         * Возвращает объекты сущности помещая их в атрибуты объекта коллекции $coll
         * 
         * @param object $coll      объект коллекции
         * @param string $class     класс сущности (название)
         * @param string $sql       запрос SQL
         * @param string $key       название поля, значение которого будет использовано в качестве ключа в коллекции
         * @return boolean
         * @throws \Stawski\Common\DBASchemaNotFoundException
         * @throws \Stawski\Common\DBADBNotFoundException
         * @throws \Stawski\Common\DBAItemNotFoundException
         */
	public function get_items_raw($coll, $class, $sql, $key = false) {
		$_instance = new $class();
		list($entity_db, $entity_schema) = $_instance->get_info();

		if(!isset($this->schemes[$entity_schema])) {
			throw new DBASchemaNotFoundException();
		}
		if(!isset($this->links[$entity_db])) {
			throw new DBADBNotFoundException('Database `'.$entity_db.'` not connected.');
		}

		$schema = $this->schemes[$entity_schema];

		$entities = $this->query_get_multi($entity_db, $sql);
		foreach($entities as $k =>  $entity) {
			$obj = new $class();
			foreach ($this->schemes[$entity_schema]['fields'] as $field => $value) {
				if($this->is_virtual_field($value)) continue;

				$obj->$field = $entity->{$this->schemes[$entity_schema]['global_prefix'].$field};
			}

			foreach ($this->schemes[$entity_schema]['fields'] as $field => $value) {
				if($value['type'] != 'concat') { continue; }
				$v = [];
				foreach ($value['concat'] as $f) {
					$v[] = $entity->{$this->schemes[$entity_schema]['global_prefix'].$f};
				}
				$obj->{$field} = implode(' ', $v);
			}

			if(!$key) {
				$coll->{$obj->id} = $obj;
			} else {
				$coll->{$obj->$key} = $obj;
			}
		}

		if(isset($schema['links']) && count($schema['links']) > 0) {
 			foreach ($schema['links'] as $field => $link_nfo) {
 				$by = $link_nfo['by'];
 				$internal = $by[0];
 				$external = $by[1];

 				$linked = new \stdClass();

				if(!isset($this->schemes[$link_nfo['schema']])) {
					throw new DBASchemaNotFoundException();
				}

 				$linked_schema = $this->schemes[$link_nfo['schema']];
 				$_ = [];
 				$_link = [];
 				foreach ($coll as $obj) {
 					$_[] = "'{$obj->$internal}'";
 					$_link[$obj->$internal] = $obj;
 				}
 				$this->get_items_raw($linked, $link_nfo['to'], "SELECT * FROM `{$linked_schema['table']}` WHERE `{$external}` IN (".implode(', ', $_).")");

 				foreach ($coll as $obj) {
 					if(isset($linked->{$obj->$internal})) {
 						$obj->{$field} = $linked->{$obj->$internal};
 					}
 				}
 			}
 		}

		return TRUE;
	}

        /**
         * Сохранение сущности в базу
         * 
         * @param object $obj   объект сущности
         * @param boolean $auto автоматически делать UPDATE
         * @return boolean
         * @throws \Stawski\Common\DBASchemaNotFoundException
         * @throws \Stawski\Common\DBADBNotFoundException
         */
	public function save_one($obj, $auto = true) {
		list($entity_db, $entity_schema) = $obj->get_info();

		if(!isset($this->schemes[$entity_schema])) {
			throw new DBASchemaNotFoundException();
		}
		if(!isset($this->links[$entity_db])) {
			throw new DBADBNotFoundException('Database `'.$entity_db.'` not connected.');
		}

		$schema = $this->schemes[$entity_schema];

		if($auto && $obj->is_real()) {
			$changes = $obj->changes();

			$SETERS = $this->generate_seters($schema, $obj, $changes);
			if($SETERS !== NULL) {
				$WHERE = $this->generate_identify($schema, $obj);
				$SQL = "UPDATE `{$this->schemes[$entity_schema]['table']}` SET ".$SETERS.' WHERE '.$WHERE.';';
				$this->query_execute($entity_db, $SQL);
			}

		} else {

			$fields = [];
			$_fields = [];
			foreach ($this->schemes[$entity_schema]['fields'] as $field => $value) {
				if($value['type'] == 'concat') continue;
				if(isset($obj->$field)) {
					$fields[] = "`{$schema['global_prefix']}{$field}`";
					$_fields[] = $field;
				}
			}

	 		$SQL = "INSERT INTO `{$schema['table']}`(".implode(',', $fields).") VALUE ".$this->_generate_values($schema, $_fields, $obj);
	 		$this->query_execute($entity_db, $SQL);
	 		$obj->id = $this->links[$entity_db]->insert_id;
	 	}

 		if(isset($schema['links']) && count($schema['links']) > 0) {
 			foreach ($schema['links'] as $field => $link_nfo) {

 				if(isset($link_nfo['readonly']) && $link_nfo['readonly'] == true) continue;

 				$internal = $link_nfo['by'][0];
 				$external = $link_nfo['by'][1];

 				if(is_array($obj->{$field})) {
 					foreach ($obj->{$field} as $value) {
 						$value->$external = $obj->$internal;
 						$value->save();
 						$value->make_snapshot();
 					}
 				} else {
 					$obj->{$field}->$external = $obj->$internal;
 					$obj->{$field}->save();
 					$obj->{$field}->make_snapshot();
 				}
 			}
 		}

 		$obj->make_snapshot();
                
        return TRUE;
	}

	private function _generate_values($schema, $fields, $obj) {
		$values = [];
		foreach ($fields as $field) {
			$values[] = $this->_value($schema['fields'][$field], $obj->$field);
		}
		return '(' . implode(', ', $values) . ')';
	}

	private function _value($field, $data) {
		if(!is_object($data) && !is_array($data) && substr($data,0,2) == "$$") return substr($data, 2);

		switch($field['type']) {
			case 'string':
				if(is_object($data)) {
					$data = $data->to_string();
				}

				if(isset($field['len'])) {
					return '\''.substr($data, 0, intval($field['len'])).'\'';
				} else {
					return '\''.$data.'\'';
				}
				break;

			case 'bool':
				return (empty($data) || $data === 0 || $data === FALSE) ? '\'0\'' : '\'1\'';
				break;

			case 'number':
				return '\''.floatval($data).'\'';
				break;

			case 'serialized':
				return '\''.serialize($data).'\'';

			default:
				return '\''.$data.'\'';
		}
	}

	public function __destruct() {
		if($this->debug) {
			echo '<!-- '.PHP_EOL.'SQL Queries: '.PHP_EOL.implode(PHP_EOL, $this->_queries).PHP_EOL.' -->'.PHP_EOL;
		}
	}

        /**
         * Возвращает схему
         * 
         * @param string $schema    идентификатор схемы
         * @return array
         * @throws DBASchemaNotFoundException
         */
	public function get_schema($schema) {
		if(!isset($this->schemes[$schema])) {
			throw new DBASchemaNotFoundException('Schema `'.$schema.'` not registered.');
		}
		
		return $this->schemes[$schema];
	}

        /**
         * Закрытие соединения с базой
         * 
         * @param string $db    идентификатор базы данных
         * @throws DBADBNotFoundException
         */
	public function close($db) {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}
		mysqli_close($this->links[$db]);
	}

        /**
         * Возвращает идентификатор первой добавленной записи
         * 
         * @param string $db    идентификатор базы данных
         * @return int
         * @throws DBADBNotFoundException
         */
	public function last_id($db) {
		if(!isset($this->links[$db])) {
			throw new DBADBNotFoundException('Database `'.$db.'` not connected.');
		}
		return intval( $this->links[$db]->insert_id );
	}

}