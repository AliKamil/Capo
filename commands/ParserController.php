<?php
/**
 * Created by Arthur Dombrovskii
 * Capo positions parser for UltimateGuitar.com
 */

namespace app\commands;

use PDOException;
use yii\console\Controller;
use yii\db\mssql\PDO;
use yii\helpers\Console;

class ParserController extends Controller
{
    protected $numerals = ['first',
                           'second',
                           'third',
                           'fourth',
                           'fifth',
                           'sixth',
                           'seventh',
                           'eighth',
                           'ninth',
                           'tenth',
                           'eleventh',
                           'twelfth'];
    //statistics
    protected $capo_entries = 0;
    protected $capo_unmatched = 0;

    /** @var PDO $source */
    protected $source;
    /** @var PDO $destination */
    protected $destination;

    private $startTime;

    /**
     * Main loop
     */
    public function actionParse()
    {
        $this->source = $this->getConnection(
            ['host' => 'devreplica.lan',
             'usr' => 'testguitar',
             'pwd' => 'zzVzz5317B',
             'port' => '3303'
            ]
        );

        $this->destination = $this->getConnection(
            ['host' => '192.168.14.243',
             'usr' => 'testguitar',
             'pwd' => 'zzVzz5317B',
             'port' => '3307'
            ]
        );
        $limit = 1000;
        $offset = 0;

        $this->start();

        $total = $this->getCount();
        $this->stdout(sprintf('Total rows to proceed: %d /n', $total));
        Console::startProgress(0, $total);
        do {
            //retrieving data from source
            $data = $this->getData($limit, $offset);
            //filtering data
            $result = $this->filter($data);
            //inserting result to destination
            $this->insert($result);

            $offset += $limit;
            Console::updateProgress($offset, $total);
        } while (!empty($data));
        Console::endProgress();
        $this->stdout(sprintf('Total "capo" occurencies (except "no capo"): %d;/n', $this->capo_entries));
        $this->stdout(sprintf('Unmatched occurencies: %d;\r\n', $this->capo_unmatched));
        $this->stdout(sprintf('Executing time: %d;\r\n', $this->stop()));
    }

    /**
     * Bulk fetch data from source database
     *
     * @param int $limit
     * @param null $offset
     * @return array
     */
    protected function getData($limit = 1000, $offset = null)
    {
        $query_string = 'SELECT t.content, t.id from content.tabs as t';
        if ($offset) {
            $query_string .= sprintf(
                ' JOIN (SELECT id FROM content.tabs ORDER BY id LIMIT %d, %d) as i ON i.id = t.id;',
                $offset,
                $limit
            );
        } else {
            $query_string .= sprintf(' LIMIT %d;', $limit);
        }
        $query = $this->source->prepare($query_string);
        $query->execute();
        return $query->fetchAll();
    }

    /**
     * Finding capo pos using regexps
     *
     * @param array $data
     * @return array
     */
    protected function filter($data)
    {
        $result = [];
        $regexp = "/capo\\D{0,10}(\\d{1,2}|" . implode('|', $this->numerals) . ")/is";
        $no_capo_regexp = "/no +capo/is";
        $capo_regex = "/( |\n)capo( |\n)/is";


        foreach ($data as $tab) {
            //first removing all 'no capo entries'
            $tab = preg_replace($no_capo_regexp, '', $tab);
            $this->capo_entries += preg_match_all($capo_regex, $tab['content']);
            //finding matches
            preg_match_all($regexp, $tab['content'], $matches);

            preg_replace($regexp, $tab['content'], '');
            $this->capo_unmatched += preg_match_all($capo_regex, $tab['content']);

            if (!empty($matches[0])) {
                $position = $matches[1][0]; //we deal only with first occurence, maybe TODO: is it a good practice?
                if (!is_numeric($position)) {
                    $position = array_search($position, $this->numerals);
                }
                if ($position > 0 and $position <= 12) { //what max capo value could be?
                    $result[] = [$tab['id'],
                                 $position];
                }
            }
        }

        return $result;
    }


    /**
     * Bulk insert data
     *
     * @param $data
     * @return int|Boolean
     * @internal param PDO $connection
     */
    protected function insert($data)
    {
        $query = 'INSERT INTO test.result (tab_id, capo_value) VALUES ';
        foreach ($data as $d) {
            $query .= '(' . $d[0] . ',' . $d[1] . '),';
        }
        return $this->destination->exec(rtrim($query, ','));

    }

    /**
     * @param array $cfg
     * @return PDO
     */
    protected function getConnection($cfg)
    {
        try {
            $host = $cfg['host'];
            $port = $cfg['port'];
            $connection = new PDO(
                "mysql:host=$host;port=$port;",
                $cfg['usr'],
                $cfg['pwd']
            );
            return $connection;
        } catch (\PDOException $err) {
            $this->stdout($err->getMessage(), Console::BG_RED);
            die;
        }
    }

    /**
     * @return string count
     */
    protected function getCount()
    {
        $query = $this->source->prepare('SELECT COUNT(*) FROM content.tabs');
        $query->execute();
        return $query->fetchColumn();
    }


    /**
     * The following part is for perfomance testing purposes only
     */


    /**
     * Start time count
     */
    protected function start()
    {
        $this->startTime = $this->microtime_float();
    }

    /**
     * Stop time count and returns script executing time
     * @return int
     */
    protected function stop()
    {
        return $this->microtime_float() - $this->startTime;
    }

    /**
     * SpeedTest
     * @param int $id
     */
    public function actionTest($id)
    {
        $this->source = $this->getConnection(
            ['host' => 'devreplica.lan',
             'usr' => 'testguitar',
             'pwd' => 'zzVzz5317B',
             'port' => '3303'
            ]
        );
        $this->start();
        $query = 'select content, id from content.tabs where id = ' . $id;
        $query = $this->source->prepare($query);
        $query->execute();
        $this->stdout(sprintf('Selecting one row by id took: %d', $this->stop()));
    }


    protected function microtime_float()
    {
        list($usec, $sec) = explode(" ", microtime());
        return ((float)$usec + (float)$sec);
    }

}