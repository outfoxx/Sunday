---
title: Sunday - Generator - Targets - Common TypeScript Target Features
---
# Common TypeScript Target Features

The following features are supported by all TypeScript code generation targets.

## Generated Types

### Scalars

RAML built-in scalar types are mapped according to the following table.

| RAML Type | TypeScript Type |
| :-: | :-: |
| any | any |
| boolean | boolean |
| number / integer | number |
| string | string |
| date-only | LocalDate (Sunday) |
| time-only | LocalTime (Sunday) |
| datetime-only | LocalDateTime (Sunday) |
| datetime | OffsetDateTime (Sunday) |
| file | ArrayBuffer |
| nil | void |

### Objects

For each RAML type that is an `object` or where the root of the inheritance tree is an `object` and contains any defined properties, a TypeScript interface and implementing class are generated.

Value constructors and fluent style copy methods are generated by default for all classes.

??? example "Example Class Generation"
    __RAML Type Definition__
    ```yaml
    %RAML 1.0
    title: Test API

    types:
      
      Item:
        type: object
        properties:
          name: String
          value: integer
    ```

    __Generated TypeScript Class__
  	```typescript
  	export interface Item {

  	  name: string;
  	  value: number;

  	}

  	export class Item implements Item {

  	  name: string;
  	  value: number;

  	  constructor(name: string, value: number) {
  	  	this.name = name;
  	  	this.value = value;
  	  }

  	  copy(src: Partial<Item>): Item {
  	  	return new Item(src.name ?? this.name, src.value ?? this.value)
  	  }

  	  toString() {
  	  	return `Item(name='${this.name}', value='${this.value}')`;
  	  }
    }
  	```

#### Simple Objects

RAML types that are "simple" objects (where no `properties` facet is defined) are mapped to the TypeScript `object` type.

??? example "Example Simple Object Generation"
    __RAML Type Definition__
    ```yaml
    %RAML 1.0
    title: Test API
    types:

      Container:
        map: object
    ```

    __Generated TypeScript Class__
    ```typescript
    export interface Container {

      map: object;

    }

    export class Container implements Container {

      map: object;

  	  constructor(map: object) {
  	  	this.map = map;
  	  }

  	  copy(src: Partial<Container>): Container {
  	  	return new Container(src.map ?? this.map)
  	  }

  	  toString() {
  	  	return `Container(map='${this.map}')`;
  	  }
    )
    ```

#### Pattern Objects

RAML types that only contain pattern properties (i.e. property names defined by regular expressions) are mapped to TypeScript built-in objects with the key as a string and the value as the type specified in the pattern (e.g. `{ [key: string]: number }`). They are generated inline and no type aliases are generated.

??? example "Example Pattern Object Generation"
    __RAML Type Definition__
    ```yaml
    %RAML 1.0
    title: Test API
    types:

      MapOfInts:
        type: object
          //: integer

      Container:
        map: MapOfInts
    ```

    __Generated TypeScript Class__ (simplified)
    ```typescript
    export interface Container {

      map: { [key: string]: number };

    }

    export class Container implements Container {

      map: { [key: string]: number };

  	  constructor(map: { [key: string]: number }) {
  	  	this.map = map;
  	  }

  	  copy(src: Partial<Container>): Container {
  	  	return new Container(src.map ?? this.map)
  	  }

  	  toString() {
  	  	return `Container(map='${this.map}')`;
  	  }
    )
    ```

#### JacksonJS Decorators

The generator defaults to adding JacksonJS JSON serialization annotations to the generated classes and interfaces. This enables advanced JSON features (e.g. polymorphism, explicit naming, etc.) to be supported without editing the generated types or registering mixins.

??? example "Example JacksonJS Annotations Generation (Explicit Names)"
    __RAML Type Definition__
    ```yaml
    %RAML 1.0
    title: Test API
    types:
      
      Item:
        type: object
        properties:
          simple-name: string
          value: integer
    ```

    __Generated TypeScript Class__
    ```typescript
    export interface Item {

      name: string;
      value: number;

    }

    export class Item implements Item {

      @JsonProperty({value: 'simple-name'})
      @JsonClassType({type: () => [String]})
      name: string;

      @JsonClassType({type: () => [Number]})
      value: number;

      constructor(name: string, value: number) {
        this.name = name;
        this.value = value;
      }

      copy(src: Partial<Item>): Item {
        return new Item(src.name ?? this.name, src.value ?? this.value)
      }

      toString() {
        return `Item(name='${this.name}', value='${this.value}')`;
      }
    }
    ```

??? example "Example JacksonJS Annotations Generation (Polymorphism)"
    __RAML Type Definition__
    ```yaml
    %RAML 1.0
    title: Test API
    types:
    
      Device:
        type: object
        discriminator: type
        properties:
          type: string
          name: string
      
      Phone:
        type: Device
        discriminatorValue: phone
        properties:
          hasGPS: boolean
      
      Tablet:
        type: Device
        discriminatorValue: tablet
        properties:
          isKeyboardAttached: boolean

    ```

    __Generated TypeScript Classes__
    ```typescript
    export interface Device {

      type: string;
      name: string;

    }

    @JsonTypeInfo({
      use: JsonTypeInfo.Id.NAME,
      include: JsonTypeInfoAs.PROPERTY,
      property: 'type'
    })
    @JsonSubTypes({
      types: [
        {class: () => eval('Phone'), name: 'phone'},
        {class: () => eval('Tablet'), name = "tablet"}
      ]
    })
    export abstract class Device implements Device {

      name: string;

      constructor(name: string) {
        this.name = name;
      }

      toString(): string {
        return `Device()`;
      }      

    }
    ```

    ---

    ```typescript
    export interface Phone extends Device {

      hasGPS: boolean;

    }

    export class Phone extends Device implements Phone {
        
      @JsonClassType({type: () => [Boolean]})
      hasGPS: boolean;

      constructor(name: string, hasGPS: boolean) {
        super(name);
        this.hasGPS = hasGPS;
      }
        
      get type(): string {
        return 'tablet';
      }
    
      copy(src: Partial<Phone>): Phone {
        return new Phone(src.name ?? this.name, src.hasGPS ?? this.hasGPS);
      }

      toString(): string {
        return `Phone(name=${this.name}, hasGPS=${this.hasGPS})`;
      }      

    }
    ```

    ---

    ```typescript
    export interface Tablet extends Device {

      isKeyboardAttached: boolean;

    }

    export class Tablet extends Device implements Tablet {
        
      @JsonClassType({type: () => [Boolean]})
      isKeyboardAttached: boolean;

      constructor(name: string, isKeyboardAttached: boolean) {
        super(name);
        this.isKeyboardAttached = isKeyboardAttached;
      }
        
      get type(): string {
        return 'tablet';
      }
    
      copy(src: Partial<Tablet>): Tablet {
        return new Tablet(src.name ?? this.name, src.isKeyboardAttached ?? this.isKeyboardAttached);
      }

      toString(): string {
        return `Tablet(name=${this.name}, isKeyboardAttached=${this.isKeyboardAttached})`;
      }      

    }

    ```

!!! note
    The generation of JacksonJS annotations can be disabled via the [JacksonJS Annotations - Type Generation Option](#type-generation-options).

!!! danger
    Disabling JacksonJS will most likely require a custom (de)serialization implementation to support the full feature set.

### Arrays

RAML array types are mapped to TypeScript's `Array` type. They are generated inline and no type aliases are generated.

??? example "Example Array Generation"
    __RAML Type Definition__
    ```yaml
    %RAML 1.0
    title: Test API
    types:
    
      Item:
        type: string

      Items:
        type: array
        items: Item

      Container:
        items: Items
    ```

    __Generated TypeScript Class__ (simplified)
    ```typescript
    export class Container {
      
      items: Array<string>;

      constructor(items: Array<string>) {
        this.items = items;
      }

    }
    ```

### Unions

RAML union types are mapped to the "nearest common ancestor" of all individual types in the union, if one exists, in all other cases the union is mapped to TypeScript's `Any` type.

??? example "Example Union Generation (Common Aancestor)"
    __RAML Type Definition__
    ```yaml
    %RAML 1.0
    title: Test API
    types:
      
      Device:
        type: object
        discriminator: type
        properties:
          type: string
          name: string
      
      Phone:
        type: Device
        discriminatorValue: phone
        properties:
          hasGPS: boolean
      
      Tablet:
        type: Device
        discriminatorValue: tablet
        properties:
          isKeyboardAttached: boolean

      UnionOfAllDevices:
        type: (Phone | Tablet)

      Container:
        type: object
        properties:
          device: UnionOfAllDevices

    ```

    1. Inline union of types with a common ancestor.


    __Generated TypeScript Class__ (simplified)
    ```typescript
    export class Container {
      
      device: Device;

      constructor(device: Device) {
        this.device = device;
      }
      
    }
    ```


## Generator Options

In addition to the [options supported by all code generations targets](../target-common-features#generator-options), this target also supports the following options:

__Type Generation Options__
:   Enable/Disable TypeScript type generation options.

	##### Supported Options
	
	__JacksonJS Decorators__
	:   Enables or Disables generation of JacksonJS decorators 
	
	__Add Generated Header__
	:   Enables or Disables adding generation headers to generated types

    | CLI Option                       | Gradle Plugin Properties  | Type    | Default |
    | -------------------------------- | ------------------------- | ------- | ------- |
	| `-disable jackson-decorators`    |                           | boolean | enabled |
	| `-disable add-generation-header` |                           | boolean | enabled |