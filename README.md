# Robot til at fremfinde og notificere om fejl i ansættelser i SD Løn

Dette projekt fokuserer på at fremfinde ansættelser der er registreret med fejl, fx i overenskomster eller løntillæg. Herefter notificerer robotten relevante modtagere gennem mails eller ServiceNow sager

## Procesoversigt 
Robotten behandler følgende fejltyper

1. **Inspirationsansættelser ikke på institutionskode "XC"** <br>
    Inspirationsansættelser skal altid være tilknyttet en XC institutionskode. Inspirationsansættelser foregår på overenskomsten 47302. Processen tjekker om der er nogle aktive ansættelser på overenskomst 47302 som ikke er på XC institutionskode. 

2. **Mangler A/B forhåndsaftale** <br>
    En række forhåndsaftaler (med løntillæg) er defineret i par, og skal altid fremgå i par. Dvs. at man ikke kan have en A-aftale uden også at skulle have en B-aftale og omvendt. Disse par er prædefinerede. Processen tjekker om nogle aktive ansættelser har en aftale fra disse prædefinerede par, uden at have den definerede "partner". <br>

3. **Overenskomster i hhv. undervisningsenheder og dagtilbud** <br>
    I undervisningsenheder må der ikke være ansættelser på overenskomsterne 46001 og 46101. I dagtilbud må der ikke være ansættelser på overenskomsterne 76001, 76101 og 77001. <br> 
    *(Flere overenskomster bliver tilføjet løbende, ligesom en foreløbig liste af accepterede overenskomster også er under udarbejdelse).*

4. **Ledere uden udløbsdato på anciennitet** <br>
    Ledere skal ansættes med en "låst" anciennitetsdato (dvs. 9999-12-31). Denne proces tjekker om ledere (defineret ved oversenskomster 45082, 45081, 46901, 45101 og 47201) har anden anciennitetsdato end den låste dato. 

## Notifikationsmuligheder
Robotten notificerer relevante modtagere om de fundne fejl. Her vælges mellem følgende muligheder

1. **Mail** <br>
    Robotten sender en mail med oplysninger om fejlen. Mailen kan sendes til en fastsat person eller til en AF fællespostkasse.

2. **ServiceNow sag** <br>
    Under udarbejdelse


## Flow
- Robotten bliver startet gennem en trigger i OpenOrchestrator, som angiver hvilken process der skal køres.
- Herefter tjekker robotten, hvilke handlinger robotten skal foretage, som angivet i den eksterne styretabel.
- Robotten aktiverer processen og tjekker om der er nogle aktive ansættelser med den pågældende fejl.
