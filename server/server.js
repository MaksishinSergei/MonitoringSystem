const express = require('express');
const helmet = require("helmet");
const { Client } = require('@elastic/elasticsearch')
const config = require('config');
const elasticConfig = config.get('elastic');

const app = express();
const PORT = 3001;

app.use(express.json({ limit: '10mb' }));
app.use(helmet());
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

const esClient = new Client({
  node: elasticConfig.host,
  auth: {
    username: elasticConfig.username,
    password: elasticConfig.password
  }
})
async function checkElasticConnection() {
      await esClient.ping();
      try{
        console.log("ES Connection is up");
        return true;
      }
      catch (error){
        console.log("ES Connection is down", error.message);
        return false;
      }
}
async function elasticCreateIndex() {
    await esClient.indices.create({
      index: "app-logs",
      body: {
        mappings: {
          properties: {
            dateTime: {type: 'date'},
            userName: {type: 'keyword'}, 
            groupName: {type: 'keyword'},
            syslogIdentifier: {type: 'keyword'},
            priority: {type: 'keyword'},
            command: {type: 'keyword'}, 
            commandLine: {type: 'text'},
            exe: {type: 'text'},
            hostName: {type: 'text'},
            processIdentifier: {type: 'keyword'}, 
            message: {type: 'text'}
          }
        }
      }
    }, { ignore: [400] });
    console.log('Индекс app-logs готов');
}

app.post('/api/logs/storage', async(req, res) => {
    try{
        const logs = req.body;
        const logObject = {
            "userName": logs.UserName || "unknown",
            "groupName": logs.GroupName || "unknown",
            "syslogIdentifier": logs.SYSLOG_IDENTIFIER || "unknown",
            "priority": logs.PRIORITY || "unknown",
            "command": logs._COMM || "unknown",
            "commandLine": logs._CMDLINE || "unknown",
            "exe": logs._EXE || "unknown",
            "hostName": logs._HOSTNAME || "unknown",
            "processIdentifier": logs._PID || "unknown",
            "timeStamp": logs.__REALTIME_TIMESTAMP || "unknown",
            "message": logs.MESSAGE || "unknown",
        }
        console.log('Получены логи:', logObject);
        try{
          const response = await esClient.index({
            index: 'app-logs',
            body: logObject,
            refresh: true
          });
          console.log('Лог отправлен в Elasticsearch');
        }
        catch (esError){
          console.error('Ошибка отправки в Elasticsearch:', esError.message);
        }
        res.status(200).json({
            status: "success",
            message: "Логи успешно получены",
            received_at: new Date().toISOString()
        });
    }
    catch (error) {
        console.error("Ошибка:", error);
        res.status(500).json({
        error: "Ошибка сервера",
        details:
            process.env.NODE_ENV === "development" ? error.message : undefined,
        });
    }
});

// // Новый endpoint для проверки статуса Elasticsearch
// app.get('/api/health', async (req, res) => {
//   try {
//     const health = await elasticClient.cluster.health();
//     const indices = await elasticClient.cat.indices({ format: 'json' });
    
//     res.json({
//       elasticsearch: {
//         status: health.status,
//         cluster_name: health.cluster_name,
//         number_of_nodes: health.number_of_nodes
//       },
//       indices: indices.body
//     });
//   } catch (error) {
//     res.status(503).json({
//       elasticsearch: {
//         status: 'unavailable',
//         error: error.message
//       }
//     });
//   }
// });

app.get('/api/logs/search', async (req, res) => {
  try {
    const { query, size = 50 } = req.query;
    const searchBody = {
      index: 'app-logs',
      body: {
        query: {
          match_all: {}
        },
        sort: [
          { timestamp: { order: 'desc' } }
        ],
        size: parseInt(size)
      }
    };

    if (query) {
      searchBody.body.query = {
        multi_match: {
          query: query,
          fields: ['userName', 'groupName', 'syslogIdentifier', 'commandLine', 'message', 'command', 'hostName', 'timeStamp']
        }
      };
    }

    const result = await elasticClient.search(searchBody);
    
    res.json({
      total: result.body.hits.total.value,
      logs: result.body.hits.hits.map(hit => ({
        id: hit._id,
        ...hit._source
      }))
    });
  } catch (error) {
    console.error('Ошибка поиска:', error);
    res.status(500).json({ error: error.message });
  }
});

app.use((req, res) => {
  res.status(404).json({ error: "Ресурс не найден" });
});

app.listen(PORT, "0.0.0.0", async () => {
  console.log(`Сервер для приема логов запущен на порту ${PORT}`);
  const isElasticConnection = await checkElasticConnection();
  if (isElasticConnection){
    await elasticCreateIndex();
  }
});