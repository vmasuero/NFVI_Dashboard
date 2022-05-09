### Operacion 🔧

_ Procesing Template File:

The Template File must be in the same local Diretory as the execution of the Containers.

```
For Linux:
	docker run -v $(pwd):/usr/app/template vmasuero/nfvi_processor:1.2 <file>.xlsx

For Windows:
	docker run -v %cd%:/usr/app/template vmasuero/nfvi_processor:1.2 <file>.xlsx
```



_ Deploying DashBoad Web:

```
For Linux:
	docker run -v $(pwd):/usr/app/template -p 8151:8151 vmasuero/nfvi_dashboard:1.3 <file>.json

For Windows:
	docker run -v %cd%:/usr/app/template -p 8151:8151 vmasuero/nfvi_dashboard:1.3 <file>.json

```

