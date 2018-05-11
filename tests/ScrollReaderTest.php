<?php 

namespace Port\Elasticsearch\Tests;

use PHPUnit\Framework\TestCase;
use Elasticsearch\Client;
use Port\Elasticsearch\ScrollReader;

class ScrollReaderTest extends TestCase
{
    protected function setup()
    {
        $this->client = $this->prophesize(Client::class);
        $this->reader = new ScrollReader($this->client, [
            'foo' => 'bar',
        ]);
    }

    public function testGetItems()
    {
        $this->client->search(['foo' => 'bar', 'scroll' => '1m'])
            ->willReturn([
                '_scroll_id' => '1',
                'hits' => [
                    'hits' => [
                        'foo' => 'bar'
                    ]
                ]
            ]);

        $this->client->search(['foo' => 'bar', 'scroll' => '1m', 'scroll_id' => '1'])
            ->willReturn([
                '_scroll_id' => '2',
                'hits' => []
            ]);

        $response = $this->reader->getItems();
        $items = [];
        
        foreach ($response as $item) {
            $items[] = $item;
        }

        $this->assertEquals(['foo' => 'bar'], $items);
    }
}