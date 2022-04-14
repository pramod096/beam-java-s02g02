## Google PageRank

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;***PageRank*** is an algorithm used by ***Google Search*** to display websites in their search engine results based on the rank of each website. PageRank is a way of measuring the importance of website pages by counting the number and quality of links to a page to determine a rough estimate of how important the website is. The underlying assumption is that more important websites are likely to receive more links from other websites.

## Algorithm

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Consider a page ***A*** has pages ***T1…Tn*** which point to it. The parameter ***d*** is a damping factor which can be set between 0 and 1. We usually set d to 0.85. Also ***C(A)*** is defined as the number of links going out of page ***A***. The PageRank of a page ***A*** can be determined as:

```
PR(A) = (1-d) + d (PR(T1)/C(T1) + … + PR(Tn)/C(Tn))
```

## Team Members



## Implementaion
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Each Team Member will implement Google PageRank using ***Apache Beam*** with ***Java SDK***, in their unique folders and will communicate with all Team Members about the individual approach and suggestions.





## Member Comments

### Pramod Gonegari




### Saikiran Reddy Gangidi



### Ramu Vallapurapu



### Venkatesh Vemula



### Vivek Drakshapally



### Narendra Gunturu
