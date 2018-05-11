<?php declare(strict_types=1);

namespace Port\ElasticSearch;

use Port\Writer\FlushableWriter;
use Elasticsearch\Client;

class BulkWriter implements FlushableWriter
{
    private $client;

    private $request;

    private $items;    

    public function __construct(Client $client, array $request = [])
    {
        $this->client = $client;
        $this->request = $request;
    }

    public function prepare()
    {
        $this->items = [];
    }

    public function writeItem(array $item)
    {        
        $this->items[] = $item;
    }

    public function finish()
    {
        $this->flush();

        $this->items = [];
    }

    public function flush()
    {        
        $req = $this->request;
        $req['body'] = $this->items;

        $this->client->bulk($req);
    }
}