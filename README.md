### Operacion 🔧

_Procesing Template File:

```
docker run -v $(pwd):/usr/app/template vmasuero/nfvi_processor:1.0 <file>.xlsx
```



_Deploying DashBoad Web:

```
docker run -v $(pwd):/usr/app/template -p 8151:8151 vmasuero/nfvi_dashboard:1.0 <file>.json
```

