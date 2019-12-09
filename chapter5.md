---
title: 'remove me please'
description: ""
---

## Drag and Drop Parsons

```yaml
type: DragAndDropExercise
key: 748f1679ec
kind: Parsons
xp: 100
```

<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Instruction 1
- Instruction 2

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- This is an example hint.
- This is an example hint.

`@solution`
```{python}
# Edit or remove this code to create your own exercise.
# This is 1 type of drag and drop exercise, there are 2 other types. See documentation:
# http://instructor-support.datacamp.com/en/articles/3039539-course-drag-drop-exercises

# Make sure you only use SPACES, NOT TABS in front of each line.
- id: a
  title: Program
  language: python
  maxOffset: 2
  items:
    - content: 'def is_true(boolean_value):'
      id: id_0
    - content: 'if boolean_value:'
      id: id_1
      offset: 1
    - content: 'return True'
      id: id_2
      offset: 2
    - content: 'return False'
      id: id_3
      offset: 1
```

`@sct`
```{python}
checks:
  - condition: check_index(id_0) == solution
    incorrectMessage: "Start by defining the function" # If that condition is not true, show this message.
  - condition: check_index(id_1) == solution
    incorrectMessage: "Start with a check on the function argument"
  - condition: check_index(id_2) == solution
    incorrectMessage: "Try again, you can do it!"
  - condition: check_index(id_3) == solution
    incorrectMessage: "We believe in you, try again!"
successMessage: 'Well done!' # Message shown when all is correct.
errorMessage: 'Try again, you can do it!' # Message shown when there are errors (and there is no specific error available).
isOrdered: true  # Should the items in the zones be ordered as in the solution code?
```

---

## Drag and Drop : Sorting

```yaml
type: DragAndDropExercise
key: f86c62a04f
kind: Order
xp: 100
```

<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Instruction 1
- Instruction 2

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- This is an example hint.
- This is an example hint.

`@solution`
```{python}
# Edit or remove this code to create your own exercise.
# This is 1 type of drag and drop exercise, there are 2 other types. See documentation:
# http://instructor-support.datacamp.com/en/articles/3039539-course-drag-drop-exercises

# Make sure you only use SPACES, NOT TABS in front of each line.
- id: data_science_process
  title: Data Science Process
  items:
    - id: question # ID of the item. This can be used in the SCTs.
      content: Ask an interesting question # Name of an item. Feel free to use markdown.
    - id: data
      content: Get the data
    - id: explore
      content: Explore the data
    - id: model
      content: Model the data
    - id: communicate
      content: Communicate and visualize the results
```

`@sct`
```{python}
checks:
  - condition: check_index(question) == solution
    incorrectMessage: 'Wrong! Try again!' # If that condition is not true, show this message.
  - condition: check_index(data) == solution
    incorrectMessage: 'Try again! You can do it!'
  - condition: check_index(explore) == solution
    incorrectMessage: 'We believe in you, try again!'
  - condition: check_index(model) == solution
    incorrectMessage: 'We believe in you, try again!'
  - condition: check_index(communicate) == solution
    incorrectMessage: 'We believe in you, try again!'
successMessage: "Congratulations" # Message shown when all is correct.
failureMessage: "Try again!" # Message shown when there are errors (and there is no specific error available).
isOrdered: true # Should the items in the zones be ordered as in the solution code?
```
